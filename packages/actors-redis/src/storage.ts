/**
 * `redisStorage` — Redis-backed `ActorStorage` with etag CAS. The same
 * Redis the cluster providers ride can hold actor state: one HASH per
 * actor under the shared `{namespace}` prefix,
 *
 *   {ns}:st:{type}<NUL>{key}    HASH { e: etag, s: state JSON }
 *
 * no TTL — durability is the point. Etags are client-minted UUIDs
 * (equality-compared only), so an etag can never be the empty string —
 * which is what lets `''` encode `expectedEtag: null` ("no record yet")
 * on the Lua side. Compare-and-set runs atomically in Redis; a mismatch
 * throws the branded `ActorStorageConflict`, the integrity floor the
 * cluster design leans on. Reminders ride this storage automatically.
 *
 * Scripts are registered with `defineCommand` (EVALSHA under the hood):
 * `save` runs on every `ctx.save()`, and shipping the script body per
 * call would be measurable bandwidth on that path.
 */
import { Redis } from 'ioredis';
import { ActorStorageConflict, type ActorStorage } from '@sigx/actors';
import type { RedisClient } from './index';

export interface RedisStorageOptions {
    /** An existing ioredis client (shared with the cluster providers). */
    client?: RedisClient;
    /**
     * Or a URL — the package constructs its own client, with
     * `enableAutoPipelining: true` (#311): every CAS in one event-loop tick
     * rides one socket write, which is where a busy host's cost lives
     * (#245). Pass `client` instead to own the options.
     */
    url?: string;
    /** Key namespace. Default `sigx`. */
    namespace?: string;
}

/** CAS save. KEYS[1] record, ARGV[1] expected etag ('' = none), ARGV[2] next etag, ARGV[3] state. */
const SAVE_CAS = `
local e = redis.call('HGET', KEYS[1], 'e')
if (e or '') ~= ARGV[1] then return 0 end
redis.call('HSET', KEYS[1], 'e', ARGV[2], 's', ARGV[3])
return 1
`;

/** CAS clear. KEYS[1] record, ARGV[1] expected etag ('' = none). Missing + '' is a no-op success. */
const CLEAR_CAS = `
local e = redis.call('HGET', KEYS[1], 'e')
if e == false then
  if ARGV[1] == '' then return 1 end
  return 0
end
if e ~= ARGV[1] then return 0 end
redis.call('DEL', KEYS[1])
return 1
`;

/** The commands `defineCommand` grafts onto the client. */
interface StorageCommands {
    sigxStSave(key: string, expected: string, next: string, state: string): Promise<number>;
    sigxStClear(key: string, expected: string): Promise<number>;
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

    // Re-registering the same script on a shared client is an overwrite, so
    // several redisStorage instances (or a redisStorage next to the cluster
    // providers) can share one connection safely.
    base.defineCommand('sigxStSave', { numberOfKeys: 1, lua: SAVE_CAS });
    base.defineCommand('sigxStClear', { numberOfKeys: 1, lua: CLEAR_CAS });
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
        const ok = await client.sigxStSave(stKey(type, key), expectedEtag ?? '', etag, json);
        if (ok !== 1) throw new ActorStorageConflict(type, key);
        return etag;
    }

    return {
        async load(type, key) {
            const [etag, state] = await client.hmget(stKey(type, key), 'e', 's');
            if (etag === null || state === null) return null;
            return { state: JSON.parse(state) as unknown, etag };
        },
        save: (type, key, state, expectedEtag) =>
            put(type, key, JSON.stringify(state), expectedEtag),
        saveText: put,
        async clear(type, key, expectedEtag) {
            const ok = await client.sigxStClear(stKey(type, key), expectedEtag ?? '');
            if (ok !== 1) throw new ActorStorageConflict(type, key);
        }
    };
}
