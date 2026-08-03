/**
 * `ActorStorage` over a Durable Object's own storage.
 *
 * A DO holds exactly ONE actor here, so the type and key are already implied
 * by which object you are talking to. They still go in the record key, so a
 * mis-routed call fails loudly instead of quietly reading another actor's
 * state.
 *
 * DO storage is strongly consistent and single-threaded per object, so the
 * read-compare-write below is atomic without a transaction — which is what
 * the runtime's etag CAS ("the integrity floor") needs.
 */
import { ActorStorageConflict, type ActorStorage, type ActorStorageRecord } from '@sigx/actors';

/**
 * `DurableObjectState.blockConcurrencyWhile` — an explicit concurrency
 * gate. A Durable Object may deliver another event while your code awaits
 * something that is not a storage operation, so any read-modify-write that
 * can run OUTSIDE an actor turn needs one.
 */
export type BlockConcurrencyWhile = <T>(fn: () => Promise<T>) => Promise<T>;

/**
 * The slice of `DurableObjectStorage` this needs. Narrow on purpose: the
 * package is testable with a plain Map, no Workers runtime required.
 */
export interface DurableStorage {
    get<T>(key: string): Promise<T | undefined>;
    put<T>(key: string, value: T): Promise<void>;
    delete(key: string): Promise<boolean>;
}

/** NUL separator, matching the runtime's own actor ids: real keys may
 *  contain `/` or `:`, so neither is safe. */
const SEP = '\u0000';
const PREFIX = 'sigx:state';

export interface DurableObjectStorageOptions {
    /**
     * `state.blockConcurrencyWhile`. Optional because the runtime already
     * serializes state writes for an actor through its turns, and one DO
     * holds one actor — so the compare-and-set below cannot interleave via
     * the normal dispatch path. Pass it for defence in depth, or if you
     * call this storage from outside the actor runtime.
     */
    blockConcurrencyWhile?: BlockConcurrencyWhile;
}

export function durableObjectStorage(
    storage: DurableStorage,
    options: DurableObjectStorageOptions = {}
): ActorStorage {
    const recordKey = (type: string, key: string): string =>
        `${PREFIX}${SEP}${type}${SEP}${key}`;
    const gate: BlockConcurrencyWhile =
        options.blockConcurrencyWhile ?? ((fn) => fn());

    return {
        async load(type, key) {
            return (await storage.get<ActorStorageRecord>(recordKey(type, key))) ?? null;
        },

        save(type, key, state, expectedEtag) {
            return gate(async () => {
            const id = recordKey(type, key);
            const current = await storage.get<ActorStorageRecord>(id);
            const actual = current?.etag ?? null;
            if (actual !== expectedEtag) throw new ActorStorageConflict(type, key);
            // `Number(x) || 0` like the core providers: a non-numeric etag
            // (corruption, or a record from another implementation) would
            // otherwise produce 'NaN' and wedge every later CAS.
            const etag = String((Number(actual) || 0) + 1);
            await storage.put<ActorStorageRecord>(id, { state, etag });
            return etag;
            });
        },

        clear(type, key, expectedEtag) {
            return gate(async () => {
            const id = recordKey(type, key);
            const current = await storage.get<ActorStorageRecord>(id);
            const actual = current?.etag ?? null;
            // Only a clear that EXPECTED nothing may no-op. With a non-null
            // expectedEtag against a missing record the caller is working
            // from a version that no longer exists — that is a conflict, and
            // letting it pass would let a stale activation wipe state
            // undetected. Same rule as memoryStorage/fileStorage.
            if (actual === null && expectedEtag === null) return;
            if (actual !== expectedEtag) throw new ActorStorageConflict(type, key);
            await storage.delete(id);
            });
        }
    };
}
