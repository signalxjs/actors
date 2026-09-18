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
 *
 * The append path (#375) keeps a record as a snapshot plus a log without
 * ever rewriting the snapshot to add to the log:
 *
 * - `<record>` holds `{ state, etag }` — the last full save, exactly as
 *   before, so a record an older version wrote loads unchanged;
 * - `<record>⟨SEP⟩head` holds the record's CURRENT etag once an append has
 *   minted one past the snapshot's (absent until then);
 * - `<record>⟨SEP⟩log⟨SEP⟩<ordinal>` holds one appended entry as its JSON
 *   text, the ordinal being the etag that append minted, zero-padded so the
 *   keys sort in append order under `storage.list({ prefix })`.
 *
 * An append `put`s the entry and the head — O(entry), whatever the state
 * weighs. A full save writes the snapshot and deletes the head and the log
 * (the compaction the seam requires); a clear deletes all three. Each group
 * of writes happens inside the adapter's gate with no non-storage await in
 * between, which a Durable Object commits atomically.
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
    /**
     * OPTIONAL: every key under `prefix`, in key order — what the append
     * path reads a record's log back with (#375). A storage without it gets
     * no `appendText`, and every append is a full save, as before.
     */
    list?<T>(options: { prefix: string }): Promise<Map<string, T>>;
}

/** NUL separator, matching the runtime's own actor ids: real keys may
 *  contain `/` or `:`, so neither is safe. */
const SEP = '\u0000';
const PREFIX = 'sigx:state';

/** Digits an entry's ordinal is padded to, so log keys sort in append order. */
const ORDINAL_DIGITS = 16;

/** The integer an etag stands for — `Number(x) || 0` like the core
 *  providers: a non-numeric etag (corruption, or a record from another
 *  implementation) would otherwise produce 'NaN' and wedge every later CAS. */
const ordinalOf = (etag: string | null | undefined): number => Number(etag) || 0;

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
    const headKey = (id: string): string => `${id}${SEP}head`;
    const logPrefix = (id: string): string => `${id}${SEP}log${SEP}`;
    const logKey = (id: string, ordinal: number): string =>
        `${logPrefix(id)}${String(ordinal).padStart(ORDINAL_DIGITS, '0')}`;
    const gate: BlockConcurrencyWhile =
        options.blockConcurrencyWhile ?? ((fn) => fn());
    const list = storage.list?.bind(storage);

    /** The record's current etag: the head an append minted, else the snapshot's own; `null` without a record. */
    const currentEtag = async (id: string, record: ActorStorageRecord | undefined): Promise<string | null> => {
        if (!record) return null;
        return (list ? await storage.get<string>(headKey(id)) : undefined) ?? record.etag;
    };

    /** Every key of `id`'s log, in append order. */
    const logKeys = async (id: string): Promise<string[]> =>
        list ? [...(await list({ prefix: logPrefix(id) })).keys()] : [];

    /** Delete the head and the log of `id` — the compaction a full save and a clear both owe. */
    const dropLog = async (id: string): Promise<void> => {
        if (!list) return;
        await Promise.all([...(await logKeys(id)), headKey(id)].map((k) => storage.delete(k)));
    };

    const adapter: ActorStorage = {
        async load(type, key) {
            const id = recordKey(type, key);
            const record = await storage.get<ActorStorageRecord>(id);
            if (!record || !list) return record ?? null;
            const etag = (await currentEtag(id, record))!;
            const from = ordinalOf(record.etag);
            const prefix = logPrefix(id);
            const log: unknown[] = [];
            for (const [k, json] of await list<string>({ prefix })) {
                // An entry at or below the snapshot's ordinal is already
                // folded into it: never replayed, whatever left it behind.
                if (Number(k.slice(prefix.length)) > from) log.push(JSON.parse(json));
            }
            return { state: record.state, etag, log };
        },

        // No `saveText`, deliberately (#238): `storage.put` takes a
        // STRUCTURED value and the platform serializes it itself, so this
        // adapter never ran the second walk the option exists to remove.
        // Handing it a string would store a JSON string it must parse back.
        save(type, key, state, expectedEtag) {
            return gate(async () => {
            const id = recordKey(type, key);
            const current = await storage.get<ActorStorageRecord>(id);
            const actual = await currentEtag(id, current);
            if (actual !== expectedEtag) throw new ActorStorageConflict(type, key);
            const etag = String(ordinalOf(actual) + 1);
            // The snapshot folds every appended entry: the log goes in the same write.
            await dropLog(id);
            await storage.put<ActorStorageRecord>(id, { state, etag });
            return etag;
            });
        },

        clear(type, key, expectedEtag) {
            return gate(async () => {
            const id = recordKey(type, key);
            const current = await storage.get<ActorStorageRecord>(id);
            const actual = await currentEtag(id, current);
            // Only a clear that EXPECTED nothing may no-op. With a non-null
            // expectedEtag against a missing record the caller is working
            // from a version that no longer exists — that is a conflict, and
            // letting it pass would let a stale activation wipe state
            // undetected. Same rule as memoryStorage/fileStorage.
            if (actual === null && expectedEtag === null) return;
            if (actual !== expectedEtag) throw new ActorStorageConflict(type, key);
            await dropLog(id);
            await storage.delete(id);
            });
        }
    };

    // The append path (#375), only where the platform can list a prefix:
    // without `list` the log could not be read back, and the host's full
    // save per append is the right answer.
    if (list) {
        adapter.appendText = (type, key, json, expectedEtag) =>
            gate(async () => {
                const id = recordKey(type, key);
                const current = await storage.get<ActorStorageRecord>(id);
                const actual = await currentEtag(id, current);
                // Nothing to append to, or a stale writer: the same brand, nothing written.
                if (actual === null || actual !== expectedEtag) throw new ActorStorageConflict(type, key);
                const ordinal = ordinalOf(actual) + 1;
                await storage.put(logKey(id, ordinal), json);
                await storage.put(headKey(id), String(ordinal));
                return String(ordinal);
            });
    }
    return adapter;
}
