/**
 * What a job READ costs (#229, from the #227 measurement).
 *
 * `JobInfo` is 8 fields that deliberately exclude the checkpoint and the
 * result — yet every method-path read built it from `ctx.snapshot()`, a
 * full encode+revive of the whole state including both. The bench ladder
 * (`jobs/status-read`) fell 681× from an empty checkpoint to 2 000 rows.
 *
 * The probe counts codec work exactly, the change-throttle way: a type
 * handler over a `Marker` held in state must be invoked on every encode
 * of the subtree that holds it, so counting its `serialize` calls counts
 * clones deterministically, with no clock involved. Markers are placed so
 * the counts attribute: one in the CHECKPOINT (must never be encoded by a
 * read) and one in `extra` (the one field a read may still have to clone).
 */
import { afterEach, describe, expect, it } from 'vitest';
import { defineJob } from '@sigx/actors/job';
import { actor, defineActor } from '@sigx/actors';
import { createHost, memoryStorage, type Host } from '@sigx/actors/host';
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

async function until(cond: () => boolean | Promise<boolean>, ms = 3000): Promise<void> {
    for (let waited = 0; !(await cond()); waited += 5) {
        if (waited > ms) throw new Error('condition not reached');
        await new Promise((r) => setTimeout(r, 5));
    }
}

let running: Host | null = null;
afterEach(async () => {
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

/** Parks holding a Marker-bearing checkpoint and a progress entry. */
const StatusJob = defineJob<null, null, Marker>({
    type: 'InfoCostJob',
    allowAnonymous: true,
    run: async (job) => {
        await job.progress({ done: 1 });
        return job.pause(new Marker());
    }
});

/** Same shape, plus declared `state:` extra carrying its OWN marker. */
const ExtraJob = defineJob<null, null, Marker, { marker: Marker; note: string }>({
    type: 'InfoExtraJob',
    allowAnonymous: true,
    state: () => ({ marker: new Marker(), note: '' }),
    run: async (job) => job.pause(new Marker())
});

async function start(actors: Parameters<typeof createHost>[0]['actors']): Promise<Host> {
    running = createHost({
        actors,
        storage: memoryStorage(),
        types: [probe],
        defaults: quiet
    });
    await running.start();
    return running;
}

describe('JobInfo without a whole-state clone (#229)', () => {
    it('status() encodes NOTHING when the definition declares no extra state', async () => {
        await start([StatusJob]);
        const client = actor(StatusJob, 'r1');
        await client.start(null);
        await until(async () => (await client.status()).status === 'paused');

        encodes = 0;
        await client.status();
        await client.status();
        await client.status();
        // The checkpoint holds the only marker: a read that clones the
        // whole state encodes it once per call (3 here); a read built off
        // live scalars encodes nothing.
        expect(encodes).toBe(0);
    });

    it('a declared extra clones ONLY the extra subtree, never the checkpoint', async () => {
        await start([ExtraJob]);
        const client = actor(ExtraJob, 'r1');
        await client.start(null);
        await until(async () => (await client.status()).status === 'paused');

        encodes = 0;
        const info = await client.status();
        // One marker lives in extra (must round-trip through the codec:
        // exactly one encode), one in the checkpoint (must not be touched:
        // a whole-state clone would make this 2).
        expect(encodes).toBe(1);
        expect(info.extra.marker).toBeInstanceOf(Marker);
    });

    it('the returned info never aliases live state', async () => {
        await start([StatusJob]);
        const client = actor(StatusJob, 'r1');
        await client.start(null);
        await until(async () => (await client.status()).status === 'paused');

        const first = await client.status();
        expect(first.progress).toEqual({ done: 1 });
        first.progress!.done = 999;
        const second = await client.status();
        expect(second.progress).toEqual({ done: 1 });
    });

    it('mutating a returned extra never reaches live state', async () => {
        await start([ExtraJob]);
        const client = actor(ExtraJob, 'r1');
        await client.start(null);
        await until(async () => (await client.status()).status === 'paused');

        const first = await client.status();
        first.extra.note = 'hacked';
        const second = await client.status();
        expect(second.extra.note).toBe('');
    });
});

const SnapActor = defineActor({
    type: 'SnapOverload',
    allowAnonymous: true,
    state: () => ({ nested: { marker: new Marker(), n: 1 } }),
    methods: (ctx) => ({
        probeClone() {
            const clone = ctx.snapshot(ctx.state.nested);
            const marker = clone.marker instanceof Marker;
            clone.n = 99;
            return { marker, cloneN: clone.n, stateN: ctx.state.nested.n };
        }
    })
});

describe('ctx.snapshot(value) — the subtree clone door (#229)', () => {
    it('clones a subtree through the host codec, detached', async () => {
        const host = await start([SnapActor]);
        const result = (await host.dispatch(
            { type: SnapActor.type, key: 'k' },
            'probeClone',
            [],
            { callChain: [], callId: 'test' }
        )) as { marker: boolean; cloneN: number; stateN: number };
        // The custom handler round-tripped (a structuredClone shortcut
        // would throw or drop it), and the clone is detached: mutating it
        // never reaches live state.
        expect(result).toEqual({ marker: true, cloneN: 99, stateN: 1 });
    });
});
