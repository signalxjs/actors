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
 *
 * `appendText` IS here (#312), parsing the one entry it is handed — small
 * by construction, and the log has to be a tree for `load` to clone it
 * alongside the state. A full save replaces the record, log included, so
 * truncation on save is the same `set` as the save itself.
 */
import { ActorStorageConflict } from '../errors';
import type { ActorStorage } from '../types';

interface MemoryRecord {
    state: unknown;
    etag: string;
    log: unknown[];
}

export function memoryStorage(): ActorStorage {
    const records = new Map<string, MemoryRecord>();
    let counter = 0;
    const id = (type: string, key: string) => `${type}\u0000${key}`;

    return {
        async load(type, key) {
            const record = records.get(id(type, key));
            return record
                ? { state: structuredClone(record.state), etag: record.etag, log: structuredClone(record.log) }
                : null;
        },
        async save(type, key, state, expectedEtag) {
            const existing = records.get(id(type, key));
            if ((existing?.etag ?? null) !== expectedEtag) {
                throw new ActorStorageConflict(type, key);
            }
            const etag = String(++counter);
            // Stored by reference — `save` takes ownership per the seam
            // contract; the load-side clone keeps the record isolated.
            records.set(id(type, key), { state, etag, log: [] });
            return etag;
        },
        async appendText(type, key, json, expectedEtag) {
            const existing = records.get(id(type, key));
            if (!existing || existing.etag !== expectedEtag) {
                throw new ActorStorageConflict(type, key);
            }
            existing.log.push(JSON.parse(json));
            return (existing.etag = String(++counter));
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
