/**
 * `ActorTurnObserver`'s trailing `call` parameter — issue #245.
 *
 * The parameter is what lets an observer correlate a turn with the dispatch
 * that caused it (`callId`, `traceparent`) — the seam a tracing exporter
 * builds its callee-side span from. It is trailing and optional-by-position
 * precisely so the five-parameter observers that predate it keep working;
 * both arities are exercised here.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineActor, type ActorCallContext, type ActorRef } from '@sigx/actors';
import { defineActorApp, type ActorApp } from '@sigx/actors/host';

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async increment(by: number) {
            ctx.state.count += by;
            await ctx.save();
            return ctx.state.count;
        }
    })
});

let running: ActorApp | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

describe('turn observer call context', () => {
    it('hands the observer the turn call context', async () => {
        const app = defineActorApp({ actors: [Counter], defaults: quiet });
        running = app;
        const host = await app.start();

        const seen: Array<{ method: string; call: ActorCallContext | undefined }> = [];
        host.observeTurns((_ref, method, _queued, _elapsed, _failed, call) => {
            seen.push({ method, call });
        });

        await host.actor(Counter, 'k1').increment(1);
        expect(seen).toHaveLength(1);
        expect(seen[0]!.method).toBe('increment');
        // The context is the dispatch's own object — callId present, chain
        // empty for an external call.
        expect(seen[0]!.call?.callId).toBeTruthy();
        expect(seen[0]!.call?.callChain).toEqual([]);
    });

    it('relays traceparent from the dispatch into the observed turn', async () => {
        const TP = '00-4bf92f3577b34da6a3ce929d0e0e4736-00f067aa0ba902b7-01';
        const app = defineActorApp({ actors: [Counter], defaults: quiet }).use({
            name: 'inject-tp',
            setup(registry) {
                registry.useDispatch((next) => ({
                    dispatch: (ref, method, args, call) =>
                        next.dispatch(ref, method, args, { ...call, traceparent: TP }),
                    ...(next.dispatchStream && {
                        dispatchStream: (ref, method, args, call) =>
                            next.dispatchStream!(ref, method, args, call)
                    })
                }));
            }
        });
        running = app;
        const host = await app.start();

        const seen: Array<string | undefined> = [];
        host.observeTurns((_ref, _method, _queued, _elapsed, _failed, call) => {
            seen.push(call?.traceparent);
        });

        await host.actor(Counter, 'k1').increment(1);
        expect(seen).toEqual([TP]);
    });

    it('keeps a five-parameter observer working, alongside a six-parameter one', async () => {
        const app = defineActorApp({ actors: [Counter], defaults: quiet });
        running = app;
        const host = await app.start();

        // The pre-#245 arity — must stay assignable and callable. Two
        // observers also forces the multi-observer fold in host.ts, which
        // now threads the sixth argument.
        const legacy: Array<{ ref: ActorRef; failed: boolean }> = [];
        host.observeTurns((ref, _method, _queued, _elapsed, failed) => {
            legacy.push({ ref, failed });
        });
        const modern: Array<ActorCallContext | undefined> = [];
        host.observeTurns((_ref, _method, _queued, _elapsed, _failed, call) => {
            modern.push(call);
        });

        await host.actor(Counter, 'k1').increment(1);
        expect(legacy).toHaveLength(1);
        expect(legacy[0]).toMatchObject({ ref: { type: 'Counter', key: 'k1' }, failed: false });
        expect(modern).toHaveLength(1);
        expect(modern[0]?.callId).toBeTruthy();
    });
});
