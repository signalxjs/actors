/**
 * What the sharded default costs per operation, as counts (#441).
 *
 * `reminders/arm-cost` gates `storage_ops_per_set` on the bench; these pin
 * the mechanism behind the number so a regression names itself:
 *
 *  - a `set` on a shard this host wrote a moment ago is ONE storage op (the
 *    CAS save against the cached table and etag), not a load and a save;
 *  - a foreign write to that shard costs one failed CAS and a reload, then
 *    the shard is treated as contended — loads before every write — until
 *    the next tick's own load re-primes it;
 *  - a `clear` of a name that is not there writes nothing;
 *  - two shards do not queue behind each other: one shard's stalled save
 *    holds no other shard's set;
 *  - the tick loads its sixteen shards concurrently, not one after another.
 *
 * The provider is bound by hand — a manual clock, a no-op `deliver`,
 * `ownsShard: () => true` — exactly as the bench binds it.
 */
import { describe, expect, it } from 'vitest';
import { type ActorStorage, type ActorStorageRecord } from '@sigx/actors';
import { manualScheduler, memoryStorage, REMINDER_TYPE } from '@sigx/actors/host';
import { ReminderService } from '../src/host/reminders';
import { reminderShardOf } from '../src/host/reminder-shards';

const TICK_MS = 1_000;
const FAR_MS = 60 * 60_000;
/** The actor-id separator (`type NUL key`), spelled without an escape so no tool mangles it. */
const NUL = String.fromCharCode(0);

interface Counts {
    loads: number;
    saves: number;
    reset(): void;
}

/** A counting decorator over `memoryStorage`, reminder records only. */
function counting(inner: ActorStorage = memoryStorage()): { storage: ActorStorage; counts: Counts; inner: ActorStorage } {
    const counts: Counts = {
        loads: 0,
        saves: 0,
        reset() {
            this.loads = 0;
            this.saves = 0;
        }
    };
    // A decorator forwards the OPTIONAL members too (the `ActorStorage`
    // contract): dropping `saveText` would silently route the provider onto
    // the two-walk `save` path and count something the real adapter never
    // does. `saveText` IS a save for the count's purposes — one CAS write.
    const { saveText, appendText } = inner;
    const storage: ActorStorage = {
        load: (type, key) => {
            if (type === REMINDER_TYPE) counts.loads++;
            return inner.load(type, key);
        },
        save: (type, key, state, expectedEtag) => {
            if (type === REMINDER_TYPE) counts.saves++;
            return inner.save(type, key, state, expectedEtag);
        },
        clear: (type, key, expectedEtag) => inner.clear(type, key, expectedEtag),
        ...(saveText
            ? {
                  saveText: (type: string, key: string, json: string, expectedEtag: string | null) => {
                      if (type === REMINDER_TYPE) counts.saves++;
                      return saveText.call(inner, type, key, json, expectedEtag);
                  }
              }
            : {}),
        ...(appendText ? { appendText: appendText.bind(inner) } : {})
    };
    return { storage, counts, inner };
}

function bind(storage: ActorStorage): { service: ReminderService; scheduler: ReturnType<typeof manualScheduler> } {
    const service = new ReminderService();
    const scheduler = manualScheduler();
    service.bind({
        storage,
        scheduler,
        tickMs: TICK_MS,
        ownsShard: () => true,
        deliver: async () => {}
    });
    return { service, scheduler };
}

/** A key whose reminders land in shard `p<n>`. */
function keyInShard(n: number): string {
    for (let i = 0; ; i++) {
        const key = `k${i}`;
        if (reminderShardOf(`Waking${NUL}${key}`) === `p${n}`) return key;
    }
}

const tick = (): Promise<void> => new Promise((r) => setTimeout(r, 0));

describe('the sharded default, as storage-op counts', () => {
    it('a set after a set on the same shard is ONE storage op', async () => {
        const { storage, counts } = counting();
        const { service } = bind(storage);
        const api = service.apiFor({ type: 'Waking', key: keyInShard(3) });
        await api.set('warm', { due: FAR_MS });
        counts.reset();
        await api.set('a', { due: FAR_MS });
        await api.set('b', { due: FAR_MS });
        expect(counts).toMatchObject({ loads: 0, saves: 2 });
    });

    it('a clear of a name that is not there writes nothing', async () => {
        const { storage, counts } = counting();
        const { service } = bind(storage);
        const api = service.apiFor({ type: 'Waking', key: keyInShard(4) });
        await api.set('warm', { due: FAR_MS });
        counts.reset();
        await api.clear('never-set');
        expect(counts.saves).toBe(0);
        await api.clear('warm');
        expect(counts.saves).toBe(1);
        expect(await api.list()).toEqual([]);
    });

    it('a foreign write costs one failed CAS and a reload, then the shard loads before every write until a tick re-primes it', async () => {
        const { storage, counts, inner } = counting();
        const { service, scheduler } = bind(storage);
        const key = keyInShard(5);
        const api = service.apiFor({ type: 'Waking', key });
        await api.set('warm', { due: FAR_MS });

        // Another host writes the same shard behind our back.
        const record = (await inner.load(REMINDER_TYPE, 'p5')) as ActorStorageRecord;
        // The same `type NUL key` shape the production table uses.
        const foreign = {
            ...(record.state as Record<string, unknown>),
            [`Waking${NUL}other`]: { z: { nextDue: Date.now() + FAR_MS } }
        };
        await inner.save(REMINDER_TYPE, 'p5', foreign, record.etag);

        counts.reset();
        await api.set('a', { due: FAR_MS });
        // The cached etag lost the CAS: one failed save, one reload, one save.
        expect(counts).toMatchObject({ loads: 1, saves: 2 });
        // Nothing was lost either way.
        expect(await api.list()).toEqual(['warm', 'a']);
        expect(Object.keys(((await inner.load(REMINDER_TYPE, 'p5'))!.state as Record<string, unknown>))).toContain('Waking0000other');

        // Contended now: the next set loads first — today's two ops.
        counts.reset();
        await api.set('b', { due: FAR_MS });
        expect(counts).toMatchObject({ loads: 1, saves: 1 });

        // A tick's own load re-primes the shard; the set after it is one op.
        service.start();
        try {
            scheduler.advance(TICK_MS);
            await tick();
            await tick();
        } finally {
            service.stop();
        }
        counts.reset();
        await api.set('c', { due: FAR_MS });
        expect(counts).toMatchObject({ loads: 0, saves: 1 });
    });

    it('one shard\'s stalled save holds no other shard\'s set', async () => {
        let release!: () => void;
        const gate = new Promise<void>((r) => (release = r));
        const base = memoryStorage();
        const storage: ActorStorage = {
            load: (type, key) => base.load(type, key),
            save: async (type, key, state, expectedEtag) => {
                if (type === REMINDER_TYPE && key === 'p1') await gate;
                return base.save(type, key, state, expectedEtag);
            },
            clear: (type, key, expectedEtag) => base.clear(type, key, expectedEtag)
        };
        const { service } = bind(storage);
        const stalled = service.apiFor({ type: 'Waking', key: keyInShard(1) }).set('slow', { due: FAR_MS });
        const other = service.apiFor({ type: 'Waking', key: keyInShard(2) }).set('fast', { due: FAR_MS });
        const raced = await Promise.race([other.then(() => 'other'), new Promise((r) => setTimeout(() => r('timeout'), 200))]);
        expect(raced).toBe('other');
        release();
        await stalled;
    });

    it('the tick loads its owned shards concurrently', async () => {
        let inFlight = 0;
        let peak = 0;
        const base = memoryStorage();
        const storage: ActorStorage = {
            load: async (type, key) => {
                if (type !== REMINDER_TYPE) return base.load(type, key);
                inFlight++;
                peak = Math.max(peak, inFlight);
                await tick();
                inFlight--;
                return base.load(type, key);
            },
            save: (type, key, state, expectedEtag) => base.save(type, key, state, expectedEtag),
            clear: (type, key, expectedEtag) => base.clear(type, key, expectedEtag)
        };
        const { service, scheduler } = bind(storage);
        service.start();
        try {
            scheduler.advance(TICK_MS);
            for (let i = 0; i < 5; i++) await tick();
            expect(peak).toBeGreaterThan(1);
        } finally {
            service.stop();
        }
    });
});
