/**
 * A boundary that both SAVES and EMITS must encode the state once, not
 * twice (#233 — the deferred half of #129, justified by #227's numbers).
 *
 * `ctx.save()` encodes for storage; a value-wanting subscriber then cost
 * a second full encode inside `#snapshot()` at the same boundary. The
 * fix reuses the save's encode: `#doSave` pre-revives the snapshot from
 * the encoded tree BEFORE storage takes ownership of it (#25).
 *
 * The probe counts encodes exactly (change-throttle style): a Marker in
 * state is serialized once per whole-state encode, never more.
 */
import { afterEach, beforeEach, describe, expect, it } from 'vitest';
import { defineActor } from '@sigx/actors';
import {
    createHost,
    manualScheduler,
    memoryStorage,
    type ActorStorage,
    type Host,
    type ManualScheduler
} from '@sigx/actors/host';
import type { TypeHandler } from '@sigx/serialize';

class Marker {}

let encodes = 0;

const probe: TypeHandler<Marker, number> = {
    name: 'marker',
    tag: '$marker',
    test: (value) => value instanceof Marker,
    serialize: () => {
        encodes++;
        return 1;
    },
    revive: () => new Marker()
};

const quiet = { sweepIntervalMs: 600_000, reminderTickMs: 600_000, callTimeoutMs: 0 };
const call = { callChain: [], callId: 'test' };

const Saver = defineActor({
    type: 'SaveEmit',
    allowAnonymous: true,
    state: () => ({ n: 0, marker: new Marker() }),
    methods: (ctx) => ({
        async bumpAndSave() {
            ctx.state.n++;
            await ctx.save();
            return ctx.state.n;
        },
        total() {
            return ctx.state.n;
        }
    }),
    streams: (ctx) => ({
        async *watch() {
            yield* ctx.changes({ initial: true });
        },
        async *watchThrottled(throttleMs: number) {
            yield* ctx.changes({ initial: true, throttleMs });
        }
    })
});

const ref = { type: Saver.type, key: 'k' };

function reader(stream: AsyncIterable<unknown>): { next(what: string): Promise<unknown> } {
    const iterator = stream[Symbol.asyncIterator]();
    return {
        async next(what) {
            const r = await Promise.race([
                iterator.next(),
                new Promise<never>((_, reject) =>
                    setTimeout(() => reject(new Error(`${what}: nothing within 2000ms`)), 2000).unref?.()
                )
            ]);
            if (r.done) throw new Error(`${what}: feed ended instead of yielding`);
            return r.value;
        }
    };
}

let running: Host | null = null;
let clock: ManualScheduler;
let storage: ActorStorage;

beforeEach(() => {
    encodes = 0;
});
afterEach(async () => {
    await running?.stop({ timeoutMs: 2000 });
    running = null;
});

async function start(): Promise<Host> {
    clock = manualScheduler();
    storage = memoryStorage();
    running = createHost({
        actors: [Saver],
        storage,
        scheduler: clock,
        types: [probe],
        defaults: quiet
    });
    await running.start();
    return running;
}

const bump = (host: Host): Promise<unknown> => host.dispatch(ref, 'bumpAndSave', [], call);

describe('save + emit encodes once (#233)', () => {
    it('a saving turn with an unthrottled subscriber costs ONE encode', async () => {
        const host = await start();
        const r = reader(host.dispatchStream!(ref, 'watch', [], call));
        await r.next('seed');

        encodes = 0;
        await bump(host);
        expect((await r.next('after save')) as { n: number }).toMatchObject({ n: 1 });
        // One encode for storage, REUSED for the snapshot — not one each.
        expect(encodes).toBe(1);

        await bump(host);
        expect((await r.next('after 2nd save')) as { n: number }).toMatchObject({ n: 2 });
        expect(encodes).toBe(2);
    });

    it('a saving turn with no subscriber still costs one encode', async () => {
        const host = await start();
        await bump(host);
        encodes = 0;
        await bump(host);
        expect(encodes).toBe(1);
    });

    it('a ticks-only ($live) watcher adds no encode to a saving turn', async () => {
        // Pins #129 against the new predicate: dispatchWatch never reads the
        // feed's value, so preparing a snapshot for it would be pure waste.
        const host = await start();
        const r = reader(host.dispatchWatch!(ref, 'total', [], call, { throttleMs: 0 }));
        expect(await r.next('initial read')).toBe(0);

        encodes = 0;
        await bump(host);
        expect(await r.next('after save')).toBe(1);
        expect(encodes).toBe(1);
    });

    it('a throttled subscriber parked inside its window adds no encode', async () => {
        const host = await start();
        const r = reader(host.dispatchStream!(ref, 'watchThrottled', [60_000], call));
        await r.next('seed');

        encodes = 0;
        await bump(host);
        // Leading emit: the window was closed, so this one carries a value.
        expect((await r.next('leading emit')) as { n: number }).toMatchObject({ n: 1 });
        expect(encodes).toBe(1);

        // Inside the window: the boundary defers, so the save's encode is
        // the only one — nothing is prepared for a deferred emit.
        await bump(host);
        expect(encodes).toBe(2);
    });

    it('the delivered snapshot aliases neither live state nor the stored record', async () => {
        const host = await start();
        const r = reader(host.dispatchStream!(ref, 'watch', [], call));
        await r.next('seed');

        await bump(host);
        const snap = (await r.next('after save')) as { n: number; marker: Marker };
        expect(snap.marker).toBeInstanceOf(Marker);

        // Mutating the delivered snapshot must reach nothing: not the live
        // state, and not the record storage now owns (#25 — the snapshot
        // must never be built from the tree handed to storage).
        snap.n = 999;
        (snap as { marker: unknown }).marker = 'clobbered';
        expect(await host.dispatch(ref, 'total', [], call)).toBe(1);
        const record = await storage.load(ref.type, ref.key);
        expect(record?.state).toMatchObject({ n: 1, marker: { $marker: 1 } });
    });
});
