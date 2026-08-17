/**
 * In-memory storage — tests and ephemeral actors. A stored value never
 * aliases live activation state: `load` hands out a structured clone (the
 * reader may mutate it freely), and `save` stores its argument by reference
 * under the seam's ownership contract — the caller hands the tree over and
 * must not mutate it afterwards (see `ActorStorage`). The host always
 * passes a codec-fresh `encodeWithHandlers` tree, so cloning it again here
 * was pure duplication (#25) — measured at ~14.5% of the `state/*` profile.
 *
 * No `saveText`, deliberately (#238): this is the adapter that genuinely
 * wants the tree. Taking a string would mean parsing it back on every load
 * — paying a walk to save one, on the one backend that never needed either.
 */
import { ActorStorageConflict } from '../errors';
import type { ActorStorage, ActorStorageRecord } from '../types';

export function memoryStorage(): ActorStorage {
    const records = new Map<string, ActorStorageRecord>();
    let counter = 0;
    const id = (type: string, key: string) => `${type}\u0000${key}`;

    return {
        async load(type, key) {
            const record = records.get(id(type, key));
            return record ? { state: structuredClone(record.state), etag: record.etag } : null;
        },
        async save(type, key, state, expectedEtag) {
            const existing = records.get(id(type, key));
            if ((existing?.etag ?? null) !== expectedEtag) {
                throw new ActorStorageConflict(type, key);
            }
            const etag = String(++counter);
            // Stored by reference — `save` takes ownership per the seam
            // contract; the load-side clone keeps the record isolated.
            records.set(id(type, key), { state, etag });
            return etag;
        },
        async clear(type, key, expectedEtag) {
            const existing = records.get(id(type, key));
            if (!existing && expectedEtag === null) return;
            if ((existing?.etag ?? null) !== expectedEtag) {
                throw new ActorStorageConflict(type, key);
            }
            records.delete(id(type, key));
        }
    };
}
