/**
 * The client live channel: one connection for the page's whole subscription
 * set, reopened when that set changes.
 *
 * Driven against a FAKE transport, because what needs pinning is the
 * connection's own behaviour — how many times it opens, which subscriber a
 * frame reaches, what a reopen re-delivers — and none of that is observable
 * through a real socket without making timing the test. The wire itself is
 * pinned end to end at the bottom of this file, and by `live-wire.test.ts` on
 * the server side.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import { createLiveChannel } from '@sigx/actors/app';
import { configureActors, fetchTransport, type ActorTransport } from '@sigx/actors/client';
import type { LiveFrame, LiveSubscription } from '../src/wire-shared';

const ENDPOINT = 'http://actors.test/_sigx/actor';
const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };
/** Debounce and backoff small enough to test, large enough to still coalesce. */
const fast = { debounceMs: 2, retryMs: 5, maxRetryMs: 20 };

/** One opened connection, plus the handles to drive it. */
interface Opened {
    subs: LiveSubscription[];
    signal: AbortSignal | undefined;
    push(frame: LiveFrame): void;
    /** A clean `done` terminator — every subscription ended server-side. */
    end(): void;
    /** A dropped body: what a proxy timeout or a host restart looks like. */
    fail(error?: Error): void;
}

function fakeTransport(): { transport: ActorTransport; opens: Opened[] } {
    const opens: Opened[] = [];
    const transport: ActorTransport = {
        name: 'fake',
        call: () => Promise.reject(new Error('not used')),
        stream(symbol, args, init) {
            const queue: LiveFrame[] = [];
            let wake: (() => void) | null = null;
            let done = false;
            let failure: Error | null = null;
            const opened: Opened = {
                subs: (args[0] ?? []) as LiveSubscription[],
                signal: init?.signal,
                push(frame) {
                    queue.push(frame);
                    wake?.();
                },
                end() {
                    done = true;
                    wake?.();
                },
                fail(error) {
                    failure = error ?? new Error('body dropped');
                    wake?.();
                }
            };
            opens.push(opened);
            expect(symbol).toBe('$live#subscribe');

            return {
                async *[Symbol.asyncIterator]() {
                    for (;;) {
                        while (queue.length > 0) yield queue.shift()!;
                        if (failure) throw failure;
                        if (done) return;
                        if (init?.signal?.aborted) return;
                        await new Promise<void>((resolve) => {
                            wake = resolve;
                            init?.signal?.addEventListener('abort', () => resolve(), {
                                once: true
                            });
                        });
                        wake = null;
                    }
                }
            };
        }
    };
    return { transport, opens };
}

const sub = (key: string, method = 'total'): { type: string; key: string; method: string } => ({
    type: 'Cart',
    key,
    method
});

/** Long enough for the debounce to fire and the connection to open. */
const settle = (ms = 20): Promise<void> => new Promise((r) => setTimeout(r, ms));

describe('the live channel', () => {
    it('opens ONE connection carrying every subscription, and fans frames out by index', async () => {
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);
        const a: unknown[] = [];
        const b: unknown[] = [];

        channel.subscribe(sub('a'), (v) => a.push(v));
        channel.subscribe(sub('b'), (v) => b.push(v));
        await settle();

        // Mounted in one burst, coalesced into one open — not one per widget.
        expect(opens).toHaveLength(1);
        expect(opens[0]!.subs).toEqual([
            { t: 'Cart', k: 'a', m: 'total' },
            { t: 'Cart', k: 'b', m: 'total' }
        ]);

        opens[0]!.push({ i: 1, v: 7 });
        await settle(5);
        expect(a).toEqual([]);
        expect(b).toEqual([7]);

        channel.close();
    });

    it('shares one subscription between subscribers, and replays the last value to a late one', async () => {
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);
        const first: unknown[] = [];
        const late: unknown[] = [];

        channel.subscribe(sub('a'), (v) => first.push(v));
        await settle();
        opens[0]!.push({ i: 0, v: 1 });
        await settle(5);

        channel.subscribe(sub('a'), (v) => late.push(v));
        await settle();

        // Same subscription: nothing new to tell the server, so no reopen —
        // and the newcomer still gets a value to render.
        expect(opens).toHaveLength(1);
        expect(channel.stats().subscriptions).toBe(1);
        expect(late).toEqual([1]);
        expect(first).toEqual([1]);

        channel.close();
    });

    it('reopens once when the set changes, and suppresses the identical re-seed', async () => {
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);
        const a: unknown[] = [];
        const b: unknown[] = [];

        channel.subscribe(sub('a'), (v) => a.push(v));
        await settle();
        opens[0]!.push({ i: 0, v: 1 });
        await settle(5);
        expect(a).toEqual([1]);

        // A second widget mounts: the set changed, so the connection has to
        // carry it. Every subscription re-seeds on open — which is what makes
        // a reconnect need no resume token, and what this suppression pays for.
        channel.subscribe(sub('b'), (v) => b.push(v));
        await settle();
        expect(opens).toHaveLength(2);
        expect(opens[0]!.signal?.aborted).toBe(true);

        opens[1]!.push({ i: 0, v: 1 }); // the redundant re-seed for 'a'
        opens[1]!.push({ i: 1, v: 5 }); // 'b' first value
        await settle(5);
        expect(a).toEqual([1]); // NOT [1, 1]
        expect(b).toEqual([5]);

        // A genuinely new value after a reopen still arrives.
        opens[1]!.push({ i: 0, v: 2 });
        await settle(5);
        expect(a).toEqual([1, 2]);

        channel.close();
    });

    it('drops an unchanged value, however it arrives', async () => {
        // Found by running the real app: a mutating turn re-runs EVERY
        // subscription on that actor, so changing a room's topic also re-emits
        // its `recent(20)` — an identical list. Delivering that is a re-render
        // (and a page-cache write) with nothing behind it, and it happens on
        // every write, not on some edge.
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);
        const messages: unknown[] = [];

        channel.subscribe(sub('a', 'recent'), (v) => messages.push(v));
        await settle();

        opens[0]!.push({ i: 0, v: [] });
        opens[0]!.push({ i: 0, v: [] }); // the topic changed; this read did not
        opens[0]!.push({ i: 0, v: ['hi'] }); // a real change still arrives
        opens[0]!.push({ i: 0, v: ['hi'] });
        await settle(5);

        expect(messages).toEqual([[], ['hi']]);
        expect(channel.stats().values).toBe(2);
        channel.close();
    });

    it('drops the subscription when its last subscriber leaves, and closes the connection', async () => {
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);

        const stopOne = channel.subscribe(sub('a'), () => {});
        const stopTwo = channel.subscribe(sub('a'), () => {});
        await settle();
        expect(opens).toHaveLength(1);

        stopOne();
        await settle();
        // Still one subscriber left: the set is unchanged, so nothing reopens.
        expect(opens).toHaveLength(1);
        expect(channel.stats().subscriptions).toBe(1);

        stopTwo();
        await settle();
        expect(channel.stats().subscriptions).toBe(0);
        expect(opens[0]!.signal?.aborted).toBe(true);
        // Nothing to watch: no replacement connection.
        expect(opens).toHaveLength(1);

        channel.close();
    });

    it('never hands a keepalive to a subscriber', async () => {
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);
        const seen: unknown[] = [];

        channel.subscribe(sub('a'), (v) => seen.push(v));
        await settle();
        opens[0]!.push({ p: 1 });
        opens[0]!.push({ i: 0, v: 3 });
        opens[0]!.push({ p: 1 });
        await settle(5);

        expect(seen).toEqual([3]);
        channel.close();
    });

    it('fails one subscription without touching the others', async () => {
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);
        const errors: Error[] = [];
        const values: unknown[] = [];

        channel.subscribe(
            sub('secret'),
            () => {},
            (error) => errors.push(error)
        );
        channel.subscribe(sub('b'), (v) => values.push(v));
        await settle();

        opens[0]!.push({ i: 0, e: { message: 'nope', status: 401 } });
        opens[0]!.push({ i: 1, v: 2 });
        await settle(5);

        expect(errors).toHaveLength(1);
        expect(errors[0]!.message).toBe('nope');
        expect((errors[0] as { status?: number }).status).toBe(401);
        // The page's other widget is still live — the whole point of failing
        // per subscription rather than per connection.
        expect(values).toEqual([2]);

        channel.close();
    });

    it('reconnects with backoff after a dropped body, and re-seeds on the new one', async () => {
        const { transport, opens } = fakeTransport();
        const failures: Error[] = [];
        const channel = createLiveChannel(() => transport, {
            ...fast,
            onError: (error) => failures.push(error)
        });
        const seen: unknown[] = [];

        channel.subscribe(sub('a'), (v) => seen.push(v));
        await settle();
        opens[0]!.push({ i: 0, v: 1 });
        await settle(5);

        opens[0]!.fail(new Error('proxy idle timeout'));
        await vi.waitFor(() => expect(opens).toHaveLength(2), { timeout: 1000 });
        expect(failures.map((e) => e.message)).toEqual(['proxy idle timeout']);
        expect(channel.stats().failures).toBe(1);

        // The catch-up read after a reconnect: the value MOVED while we were
        // away, so it is not a redundant re-seed and must be delivered.
        opens[1]!.push({ i: 0, v: 9 });
        await settle(5);
        expect(seen).toEqual([1, 9]);

        channel.close();
    });

    it('does not reopen after a clean end — that would spin', async () => {
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);

        channel.subscribe(
            sub('a'),
            () => {},
            () => {}
        );
        await settle();
        // Every subscription ended server-side (all rejected, say). Reopening
        // would immediately end again, forever.
        opens[0]!.end();
        await settle(40);

        expect(opens).toHaveLength(1);
        expect(channel.stats().failures).toBe(0);
        channel.close();
    });

    it('close() aborts the connection and drops everything', async () => {
        const { transport, opens } = fakeTransport();
        const channel = createLiveChannel(() => transport, fast);

        channel.subscribe(sub('a'), () => {});
        await settle();
        channel.close();
        await settle();

        expect(opens[0]!.signal?.aborted).toBe(true);
        expect(channel.stats().subscriptions).toBe(0);
        // A subscribe after close is inert rather than an error: an app tearing
        // down can race a component mounting.
        expect(channel.subscribe(sub('b'), () => {})).toBeTypeOf('function');
        await settle();
        expect(opens).toHaveLength(1);
    });

    it('defers to a transport that brings its own push channel', async () => {
        // The seam: `ActorTransport.live()` is how a WebSocket transport would
        // ship, and it must win over the NDJSON default without any call site
        // changing.
        const calls: string[] = [];
        const transport: ActorTransport = {
            name: 'ws-ish',
            call: () => Promise.reject(new Error('not used')),
            stream() {
                calls.push('stream');
                throw new Error('the default channel must not be used');
            },
            live: () => ({
                subscribe(s, onValue) {
                    calls.push(`subscribe:${s.key}`);
                    onValue('from the transport');
                    return () => calls.push('unsubscribe');
                }
            })
        };
        const channel = createLiveChannel(() => transport, fast);
        const seen: unknown[] = [];

        const stop = channel.subscribe(sub('a'), (v) => seen.push(v));
        await settle();
        stop();

        expect(calls).toEqual(['subscribe:a', 'unsubscribe']);
        expect(seen).toEqual(['from the transport']);
        channel.close();
    });

    it('degrades to a no-op against a transport that cannot stream', async () => {
        // `stream` is required by the interface, so this is the JS / `any`
        // path. Registering the subscription anyway would schedule a reopen
        // that throws inside the run loop, which the retry logic reads as a
        // dropped connection — an endless reconnect against a transport that
        // can never serve one.
        const warn = vi.spyOn(console, 'warn').mockImplementation(() => {});
        const transport = {
            name: 'call-only',
            call: () => Promise.resolve(null)
        } as unknown as ActorTransport;
        const channel = createLiveChannel(() => transport, fast);

        const stop = channel.subscribe(sub('a'), () => {});
        await settle(40);

        expect(channel.stats().subscriptions).toBe(0);
        expect(channel.stats().failures).toBe(0);
        expect(warn.mock.calls.flat().join(' ')).toMatch(/no stream\(\) and no live\(\)/);
        stop();
        channel.close();
        warn.mockRestore();
    });

    it('is inert with no transport, which is what makes it safe during SSR', async () => {
        const channel = createLiveChannel(() => null, fast);
        const stop = channel.subscribe(sub('a'), () => {
            throw new Error('nothing can push during a server render');
        });
        await settle();
        expect(channel.stats().opens).toBe(0);
        stop();
        channel.close();
    });
});

// ---------------------------------------------------------------------------
// The same channel against the REAL `$live` mount.

const Cart = defineActor({
    type: 'Cart',
    allowAnonymous: true,
    state: () => ({ items: [] as string[] }),
    methods: (ctx) => ({
        async total() {
            return ctx.state.items.length;
        },
        async add(item: string) {
            ctx.state.items.push(item);
            await ctx.save();
            return ctx.state.items.length;
        }
    })
});

const running: Host[] = [];

afterEach(async () => {
    configureActors(null);
    for (const s of running.splice(0)) await s.stop({ timeoutMs: 1000 });
});

describe('the live channel over the wire', () => {
    it('pushes an actor read as it changes', async () => {
        const host = createHost({ actors: [Cart], defaults: quiet });
        running.push(host);
        await host.start();

        const transport = fetchTransport({
            endpoint: ENDPOINT,
            fetch: async (input, init) => {
                const request = new Request(input, init);
                const response = await handleActorRequest(request, {
                    host,
                    origin: false,
                    // No keepalive: the test would otherwise have to tell a
                    // ping apart from a value, which the reader already does.
                    streamPingMs: 0
                });
                if (!response.body || !init?.signal) return response;
                // Real fetch cancels the body on abort; an in-memory handler
                // call has no such link, so recreate it — otherwise closing
                // the channel would leave the watch (and its keep-alive) open.
                const reader = response.body.getReader();
                init.signal.addEventListener('abort', () => void reader.cancel().catch(() => {}));
                return new Response(
                    new ReadableStream<Uint8Array>({
                        async pull(controller) {
                            const { value, done } = await reader.read();
                            if (done) controller.close();
                            else controller.enqueue(value);
                        },
                        cancel: (reason) => reader.cancel(reason)
                    }),
                    { status: response.status, headers: response.headers }
                );
            }
        });

        const channel = createLiveChannel(() => transport, fast);
        const seen: unknown[] = [];
        channel.subscribe({ type: 'Cart', key: 'w1', method: 'total' }, (v) => seen.push(v));

        // The seed, then one value per mutating turn.
        await vi.waitFor(() => expect(seen).toEqual([0]), { timeout: 2000 });
        await host.actor(Cart, 'w1').add('apple');
        await vi.waitFor(() => expect(seen).toEqual([0, 1]), { timeout: 2000 });

        channel.close();
        // The watch is released, so the actor can be collected again.
        await vi.waitFor(
            () => expect(host.activations().map((a) => a.keptAlive)).toEqual([false]),
            { timeout: 2000 }
        );
    });
});

describe('a transport that brings its own channel (#102)', () => {
    /** A transport whose `live()` builds an observable "connection". */
    function delegatingTransport(): {
        transport: ActorTransport;
        liveCalls: number[];
        subscribed: unknown[];
        closes: number[];
    } {
        const liveCalls: number[] = [];
        const subscribed: unknown[] = [];
        const closes: number[] = [];
        const transport: ActorTransport = {
            name: 'fake-socket',
            call: () => Promise.reject(new Error('not used')),
            stream: () => {
                throw new Error('not used');
            },
            live: () => {
                liveCalls.push(1);
                return {
                    subscribe(sub, onValue) {
                        subscribed.push(sub);
                        void onValue;
                        return () => {};
                    }
                };
            },
            close: () => {
                closes.push(1);
            }
        };
        return { transport, liveCalls, subscribed, closes };
    }

    it('close() releases the delegate connection it resolved', async () => {
        const { transport, liveCalls, subscribed, closes } = delegatingTransport();
        // No plugin anywhere: the module-scope `configureActors(transport)`
        // shape of #102, where nothing else owns the transport.
        const channel = createLiveChannel(() => transport, fast);
        channel.subscribe(sub('a'), () => {});
        expect(liveCalls).toHaveLength(1);
        expect(subscribed).toHaveLength(1);
        channel.close();
        // The channel resolved the delegate, so the channel releases it.
        expect(closes).toHaveLength(1);
    });

    it('close() without ever delegating closes nothing', () => {
        const { transport, closes } = delegatingTransport();
        const channel = createLiveChannel(() => transport, fast);
        // No subscription — `live()` was never called, no connection exists,
        // and a transport this channel never used must not be torn down.
        channel.close();
        expect(closes).toHaveLength(0);
    });
});
