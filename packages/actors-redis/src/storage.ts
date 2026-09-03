/**
 * `redisStorage` — Redis-backed `ActorStorage` with etag CAS. The same
 * Redis the cluster providers ride can hold actor state: one HASH per
 * actor under the shared `{namespace}` prefix, plus one LIST for the
 * append log (#312),
 *
 *   {ns}:st:{type}<NUL>{key}    HASH { e: etag, s: state JSON }
 *   {ns}:sl:{type}<NUL>{key}    LIST [ entry JSON, … ]  (oldest first)
 *
 * Its own PREFIX, not a suffix on the record key: actor keys are opaque,
 * so `{key}:l` is a key some other actor may have, and a suffix would make
 * one actor's log another actor's record.
 *
 * no TTL — durability is the point. Etags are client-minted UUIDs
 * (equality-compared only), so an etag can never be the empty string —
 * which is what lets `''` encode `expectedEtag: null` ("no record yet")
 * on the Lua side. Compare-and-set runs atomically in Redis; a mismatch
 * throws the branded `ActorStorageConflict`, the integrity floor the
 * cluster design leans on. Reminders ride this storage automatically.
 *
 * Every operation is ONE script over both keys — the CAS verdict, the
 * hash write and the list it governs (`RPUSH` on append, `DEL` on a full
 * save or clear) commit as one atomic step, and `load` returns the etag,
 * the state and the whole list in a single round trip. Scripts are
 * registered with `defineCommand` (EVALSHA under the hood): `save` runs on
 * every `ctx.save()`, and shipping the script body per call would be
 * measurable bandwidth on that path. (A Redis Cluster deployment would
 * need the two keys in one hash slot; the key layout carries no hash tag
 * today, as the membership providers' MULTIs do not either.)
 */
import { Redis } from 'ioredis';
import { ActorStorageConflict, type ActorStorage } from '@sigx/actors';
import type { RedisClient } from './index';

export interface RedisStorageOptions {
    /** An existing ioredis client (shared with the cluster providers). */
    client?: RedisClient;
    /**
     * Or a URL — the package constructs its own client, with
     * `enableAutoPipelining: true` (#311): ioredis coalesces commands
     * issued in the same event-loop tick into fewer socket writes, which
     * is where a host busy with many same-tick CAS saves spends. Pass
     * `client` instead to own the options.
     */
    url?: string;
    /** Key namespace. Default `sigx`. */
    namespace?: string;
}

/**
 * CAS save. KEYS[1] record, KEYS[2] log, ARGV[1] expected etag ('' = none),
 * ARGV[2] next etag, ARGV[3] state. A full save is the compaction: the log
 * goes with it.
 */
const SAVE_CAS = `
local e = redis.call('HGET', KEYS[1], 'e')
if (e or '') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'e', ARGV[2], 's', ARGV[3])
redis.call('DEL', KEYS[2])
return 1
`;

/**
 * CAS append (#312). KEYS[1] record, KEYS[2] log, ARGV[1] expected etag
 * (never ''), ARGV[2] next etag, ARGV[3] entry. A missing record is a
 * mismatch: there is nothing to append to.
 */
const APPEND_CAS = `
local e = redis.call('HGET', KEYS[1], 'e')
if e == false or e ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'e', ARGV[2])
redis.call('RPUSH', KEYS[2], ARGV[3])
return 1
`;

/** CAS clear. KEYS[1] record, KEYS[2] log, ARGV[1] expected etag ('' = none). Missing + '' is a no-op success. */
const CLEAR_CAS = `
local e = redis.call('HGET', KEYS[1], 'e')
if e == false then
  if ARGV[1] == '' then return 1 end
  return 0
end
if e ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1], KEYS[2])
return 1
`;

/** Load. KEYS[1] record, KEYS[2] log. nil when there is no record, else { etag, state, entries }. */
const LOAD = `
local r = redis.call('HMGET', KEYS[1], 'e', 's')
if r[1] == false then return nil end
return { r[1], r[2], redis.call('LRANGE', KEYS[2], 0, -1) }
`;

/** The commands `defineCommand` grafts onto the client. */
interface StorageCommands {
    sigxStLoad(key: string, log: string): Promise<[string, string | null, string[]] | null>;
    sigxStSave(key: string, log: string, expected: string, next: string, state: string): Promise<number>;
    sigxStAppend(key: string, log: string, expected: string, next: string, entry: string): Promise<number>;
    sigxStClear(key: string, log: string, expected: string): Promise<number>;
}

export function redisStorage(options: RedisStorageOptions): ActorStorage {
    const base =
        options.client ??
        (options.url !== undefined
            ? new Redis(options.url, { enableAutoPipelining: true })
            : undefined);
    if (!base) {
        throw new Error('[sigx actors-redis] pass either `client` (ioredis) or `url`.');
    }
    const ns = options.namespace ?? 'sigx';
    // actorId embeds a NUL — Redis keys are binary-safe, no escaping layer.
    const stKey = (type: string, key: string): string => `${ns}:st:${type}\u0000${key}`;
    // A sibling prefix (see the header): the log of `k` and the record of
    // `k:l` must be different keys, and only a prefix of its own does that.
    const logKey = (type: string, key: string): string => `${ns}:sl:${type}\u0000${key}`;

    // Re-registering the same script on a shared client is an overwrite, so
    // several redisStorage instances (or a redisStorage next to the cluster
    // providers) can share one connection safely.
    base.defineCommand('sigxStLoad', { numberOfKeys: 2, lua: LOAD });
    base.defineCommand('sigxStSave', { numberOfKeys: 2, lua: SAVE_CAS });
    base.defineCommand('sigxStAppend', { numberOfKeys: 2, lua: APPEND_CAS });
    base.defineCommand('sigxStClear', { numberOfKeys: 2, lua: CLEAR_CAS });
    const client = base as RedisClient & StorageCommands;

    // The one CAS call, reached with JSON either way — from the host's own
    // single-walk emitter via `saveText`, or from a tree `save` stringifies
    // itself (#238). The Lua script always wanted a string; only the walk
    // that produced it changes. A local function rather than `this.saveText`,
    // so the two halves stay wired together even if the storage is
    // destructured.
    async function put(
        type: string,
        key: string,
        json: string,
        expectedEtag: string | null
    ): Promise<string> {
        const etag = globalThis.crypto.randomUUID();
        const ok = await client.sigxStSave(stKey(type, key), logKey(type, key), expectedEtag ?? '', etag, json);
        if (ok !== 1) throw new ActorStorageConflict(type, key);
        return etag;
    }

    return {
        async load(type, key) {
            const row = await client.sigxStLoad(stKey(type, key), logKey(type, key));
            if (row === null || row[1] === null) return null;
            return {
                state: JSON.parse(row[1]) as unknown,
                etag: row[0],
                log: row[2].map((entry) => JSON.parse(entry) as unknown)
            };
        },
        save: (type, key, state, expectedEtag) =>
            put(type, key, JSON.stringify(state), expectedEtag),
        saveText: put,
        async appendText(type, key, json, expectedEtag) {
            const etag = globalThis.crypto.randomUUID();
            const ok = await client.sigxStAppend(stKey(type, key), logKey(type, key), expectedEtag, etag, json);
            if (ok !== 1) throw new ActorStorageConflict(type, key);
            return etag;
        },
        async clear(type, key, expectedEtag) {
            const ok = await client.sigxStClear(stKey(type, key), logKey(type, key), expectedEtag ?? '');
            if (ok !== 1) throw new ActorStorageConflict(type, key);
        }
    };
}
