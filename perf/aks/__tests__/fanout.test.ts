/**
 * The `Fanout` workload's load-bearing assumptions (#172).
 *
 * Two of them, and the whole WebSocket scale test is built on both:
 *
 *  1. **A publish with no `ctx.save()` still notifies subscribers.** The
 *     no-persist arm exists to separate delivery cost from one Redis CAS
 *     per publish; if a mutation only reached watchers via the save path,
 *     that arm would measure nothing and quietly report zero deliveries.
 *  2. **Consulting `ctx.principal` splits the shared watch loop.** The
 *     per-principal arm is the measurement of what identity costs a live
 *     fan-out (#121/#138) — it only means something if `current()` and
 *     `mine()` really do behave differently.
 *
 * Unlike `infra.test.ts` these are pure unit tests: no cluster, no Redis, no
 * deployment. `actors.app.ts` falls back to `memoryStorage()` with no
 * `REDIS_URL`, which is exactly the "embedding the app module directly"
 * case its header describes.
 */
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import { stubServerApp } from '@sigx/server/testing';
import type { ActorCallContext, Host } from '@sigx/actors';
import { principalCodec } from '../src/server-app.ts';
import { app } from '../src/actors.app.ts';
import { Fanout } from '../src/fanout.actor.ts';

const call = (overrides: Partial<ActorCallContext> = {}): ActorCallContext => ({
    callChain: [],
    callId: `test-${Math.random().toString(36).slice(2)}`,
    ...overrides
});

// ONE host for the file. `app` is module scope in `actors.app.ts` and
// `withActors()` refuses a second registry, so a per-test host is not
// available here — every case uses its own actor key instead.
let host: Host;

beforeAll(async () => {
    // AFTER the suite-wide stamp in `vitest.setup.ts`, which runs in its own
    // `beforeAll` and would otherwise decode every principal to its shape
    // rather than this rig's. `authenticate` returns null because these
    // cases drive identity explicitly through the call context; every actor
    // here is `allowAnonymous`, so nothing is denied.
    stubServerApp({ authenticate: () => null, codec: principalCodec });
    host = await app.withActors([Fanout]).start();
});

afterAll(async () => {
    await host.stop();
});

/** Read the next value, refusing to hang the suite if none arrives. */
async function nextWithin<T>(
    iterator: AsyncIterator<T>,
    ms: number,
    what: string
): Promise<T> {
    const timeout = new Promise<never>((_, reject) =>
        setTimeout(() => reject(new Error(`timed out waiting for ${what}`)), ms).unref()
    );
    const { value, done } = await Promise.race([iterator.next(), timeout]);
    if (done) throw new Error(`the watch ended while waiting for ${what}`);
    return value as T;
}

describe('Fanout', () => {
    it('notifies a watcher for a publish that never saves', async () => {
        const h = host;
        const ref = { type: 'Fanout', key: 'no-persist' };
        const iterator = h
            .dispatchWatch!(ref, 'current', [], call())
            [Symbol.asyncIterator]();
        try {
            const initial = (await nextWithin(iterator, 2000, 'the initial value')) as {
                seq: number;
            };
            expect(initial.seq).toBe(0);

            // persist: false — the arm that exists to keep storage off the
            // delivery path. The dirty mark is set by state tracking at
            // write time and folded at the turn boundary, so this must
            // reach the watcher without a save.
            await h.dispatch(ref, 'publish', [8, false], call());

            const pushed = (await nextWithin(iterator, 2000, 'the un-saved publish')) as {
                seq: number;
                payload: string;
            };
            expect(pushed.seq).toBe(1);
            expect(pushed.payload).toHaveLength(8);
        } finally {
            await iterator.return?.(undefined);
        }
    });

    it('notifies a watcher for a publish that saves', async () => {
        const h = host;
        const ref = { type: 'Fanout', key: 'persist' };
        const iterator = h
            .dispatchWatch!(ref, 'current', [], call())
            [Symbol.asyncIterator]();
        try {
            await nextWithin(iterator, 2000, 'the initial value');
            await h.dispatch(ref, 'publish', [4, true], call());
            const pushed = (await nextWithin(iterator, 2000, 'the saved publish')) as {
                seq: number;
            };
            expect(pushed.seq).toBe(1);
        } finally {
            await iterator.return?.(undefined);
        }
    });

    it('caps the payload rather than allocating whatever it is asked for', async () => {
        const h = host;
        const ref = { type: 'Fanout', key: 'capped' };
        await h.dispatch(ref, 'publish', [1 << 30, false], call());
        const value = (await h.dispatch(ref, 'current', [], call())) as { payload: string };
        expect(value.payload).toHaveLength(1 << 20);
    });

    it('serves `mine()` per principal while `current()` is shared', async () => {
        const h = host;
        const ref = { type: 'Fanout', key: 'identity' };
        await h.dispatch(ref, 'publish', [0, false], call());

        // `mine()` reads ctx.principal, so two principals must see their own
        // name — this is the read whose watch loop the runtime qualifies.
        const ada = (await h.dispatch(ref, 'mine', [], call({ principal: 'ada' }))) as {
            who: string | null;
        };
        const bob = (await h.dispatch(ref, 'mine', [], call({ principal: 'bob' }))) as {
            who: string | null;
        };
        expect(ada.who).toBe('ada');
        expect(bob.who).toBe('bob');

        // `current()` never consults identity, so it is the arm that shares
        // one loop across every subscriber.
        const shared = (await h.dispatch(ref, 'current', [], call({ principal: 'ada' }))) as {
            seq: number;
        };
        expect(shared).not.toHaveProperty('who');
        expect(shared.seq).toBe(1);
    });
});
