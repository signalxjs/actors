import { describe, expect, it } from 'vitest';
import { defineActor, isStorageConflict } from '@sigx/actors';
import { createHost, memoryStorage } from '@sigx/actors/host';

/**
 * Pins for the memoryStorage/`ActorStorage` seam contract (#25): `load`
 * returns a record the caller may freely mutate; `save` takes ownership of
 * the state tree (the caller must not mutate it after the call). Written
 * BEFORE the save-side clone was dropped — everything here must hold on
 * either side of that change.
 */

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

describe('memoryStorage contract', () => {
    it('load returns an isolated tree — mutating it never touches the stored record', async () => {
        const storage = memoryStorage();
        await storage.save('T', 'k', { rows: [{ n: 1 }] }, null);
        const first = await storage.load('T', 'k');
        (first!.state as { rows: { n: number }[] }).rows[0]!.n = 99;
        const second = await storage.load('T', 'k');
        expect(second!.state).toEqual({ rows: [{ n: 1 }] });
    });

    it('etag conflict on save and clear rejects with the storage-conflict brand', async () => {
        const storage = memoryStorage();
        const etag = await storage.save('T', 'k', { n: 0 }, null);
        await expect(storage.save('T', 'k', { n: 1 }, 'stale')).rejects.toSatisfy(
            isStorageConflict
        );
        await expect(storage.clear('T', 'k', 'stale')).rejects.toSatisfy(isStorageConflict);
        await storage.clear('T', 'k', etag);
        await expect(storage.load('T', 'k')).resolves.toBeNull();
    });

    it('a stored record never aliases live activation state', async () => {
        // The invariant the docstring promises, held up by the HOST side of
        // the seam: `saveState` hands storage a codec-fresh tree
        // (`encodeWithHandlers` allocates a new node for every mutable
        // value), so post-save mutations of live state must never leak into
        // the stored record. This test is the guard against a future
        // serialize identity fast path silently reintroducing aliasing —
        // if `encode` ever returns an already-JSON-safe tree by reference,
        // this fails and the save-side defensive clone must come back.
        const storage = memoryStorage();
        const probe = defineActor({
            type: 'Probe',
            unguarded: true,
            state: () => ({ rows: [{ n: 0 }] }),
            methods: (ctx) => ({
                async bumpAndSave() {
                    ctx.state.rows[0]!.n = 1;
                    await ctx.save();
                },
                async bumpWithoutSave() {
                    ctx.state.rows[0]!.n = 2;
                }
            })
        });
        const host = createHost({ actors: [probe], storage, defaults: quiet });
        const client = host.actor(probe, 'p');
        await client.bumpAndSave();
        await client.bumpWithoutSave();
        const record = await storage.load('Probe', 'p');
        expect(record!.state).toEqual({ rows: [{ n: 1 }] });
        await host.stop();
    });
});
