/**
 * In-memory storage — tests and ephemeral actors. Records are structured
 * clones so a stored value never aliases live activation state.
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
            records.set(id(type, key), { state: structuredClone(state), etag });
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
