/**
 * `@sigx/actors-redis` — Redis-backed cluster providers for
 * `@sigx/actors/cluster`: TTL-heartbeat membership and the claim
 * directory. Redis ≥ 7; ioredis is a peer dependency (the compare-ops are
 * Lua scripts, which need a concrete client API).
 *
 * Key layout under `{namespace}` (default `sigx`):
 *
 *   {ns}:silo:{siloId}    HASH {d: descriptor JSON}   PX ttlMs, renewed each beat
 *   {ns}:silos            SET of siloIds              lazily pruned on refresh
 *   {ns}:mver             INCR'd version counter      cheap poll compare
 *   {ns}:dir:{actorId}    "siloId\nactivationId"      no TTL — validity is the
 *                                                     owner's liveness
 *
 * Directory entries carry no TTL by design: one heartbeat per silo, not per
 * activation. The storage etag CAS in the actor runtime remains the
 * integrity floor underneath all of this.
 */
import { Redis } from 'ioredis';
import type {
    ActorDirectory,
    ClusterMembership,
    ClusterProviders,
    DirectoryEntry,
    MembershipView,
    SiloDescriptor,
    SiloStatus
} from '@sigx/actors/cluster';

/** The subset of ioredis this package calls — accepts a shared app client. */
export type RedisClient = Redis;

export interface RedisClusterOptions {
    /** An existing ioredis client (shared with the app). */
    client?: RedisClient;
    /** Or a URL — the package constructs its own client. */
    url?: string;
    /** Key namespace. Default `sigx`. */
    namespace?: string;
    /** Heartbeat cadence, ms. Default 5000. */
    heartbeatMs?: number;
    /** Heartbeat key TTL, ms (missed beats past this = dead). Default 15000. */
    ttlMs?: number;
    /** Membership view poll cadence, ms. Default 5000. */
    pollMs?: number;
}

interface Resolved {
    client: RedisClient;
    ns: string;
    heartbeatMs: number;
    ttlMs: number;
    pollMs: number;
}

function resolve(options: RedisClusterOptions): Resolved {
    const client =
        options.client ??
        (options.url !== undefined ? new Redis(options.url) : undefined);
    if (!client) {
        throw new Error('[sigx actors-redis] pass either `client` (ioredis) or `url`.');
    }
    return {
        client,
        ns: options.namespace ?? 'sigx',
        heartbeatMs: options.heartbeatMs ?? 5_000,
        ttlMs: options.ttlMs ?? 15_000,
        pollMs: options.pollMs ?? 5_000
    };
}

/** Membership + directory for ONE silo. */
export function redisCluster(options: RedisClusterOptions): ClusterProviders {
    const resolved = resolve(options);
    return {
        membership: redisMembership(resolved.client, {
            namespace: resolved.ns,
            heartbeatMs: resolved.heartbeatMs,
            ttlMs: resolved.ttlMs,
            pollMs: resolved.pollMs
        }),
        directory: redisDirectory(resolved.client, { namespace: resolved.ns })
    };
}

// ---------------------------------------------------------------------------
// Membership

const noop = (): void => {};

export function redisMembership(
    client: RedisClient,
    options: Omit<RedisClusterOptions, 'client' | 'url'> & { namespace?: string } = {}
): ClusterMembership {
    const { ns, heartbeatMs, ttlMs, pollMs } = {
        ns: options.namespace ?? 'sigx',
        heartbeatMs: options.heartbeatMs ?? 5_000,
        ttlMs: options.ttlMs ?? 15_000,
        pollMs: options.pollMs ?? 5_000
    };
    const setKey = `${ns}:silos`;
    const mverKey = `${ns}:mver`;
    const siloKey = (id: string): string => `${ns}:silo:${id}`;

    let self: SiloDescriptor | null = null;
    let cached: MembershipView = { version: 0, silos: [] };
    let beat: ReturnType<typeof setInterval> | null = null;
    let poll: ReturnType<typeof setInterval> | null = null;
    let lastOkMs = 0;
    let suspected = false;
    const changeCbs = new Set<(view: MembershipView) => void>();
    const suspectCbs = new Set<() => void>();

    const writeSelf = async (): Promise<void> => {
        if (!self) return;
        await client
            .multi()
            .hset(siloKey(self.siloId), 'd', JSON.stringify(self))
            .pexpire(siloKey(self.siloId), ttlMs)
            .exec();
        lastOkMs = Date.now();
        suspected = false;
    };

    const bumpVersion = (): Promise<unknown> => client.incr(mverKey);

    const refresh = async (): Promise<MembershipView> => {
        const [verRaw, ids] = await Promise.all([client.get(mverKey), client.smembers(setKey)]);
        const pipeline = client.pipeline();
        for (const id of ids) pipeline.hget(siloKey(id), 'd');
        const rows = (await pipeline.exec()) ?? [];
        const silos: SiloDescriptor[] = [];
        const dead: string[] = [];
        rows.forEach(([err, value], i) => {
            if (err || typeof value !== 'string') {
                dead.push(ids[i]!);
                return;
            }
            try {
                silos.push(JSON.parse(value) as SiloDescriptor);
            } catch {
                dead.push(ids[i]!);
            }
        });
        if (dead.length > 0) {
            // Lazy prune: expired heartbeats leave set members behind.
            await client.srem(setKey, ...dead).catch(noop);
        }
        const next: MembershipView = { version: Number(verRaw ?? 0), silos };
        const changed = next.version !== cached.version || dead.length > 0;
        cached = next;
        if (changed) for (const cb of changeCbs) cb(next);
        return next;
    };

    return {
        async join(descriptor) {
            self = descriptor;
            await writeSelf();
            await client.sadd(setKey, descriptor.siloId);
            await bumpVersion();
            await refresh();
            beat = setInterval(() => {
                void writeSelf().catch(() => {
                    // Can't prove our own membership past the TTL → fence.
                    if (!suspected && Date.now() - lastOkMs > ttlMs) {
                        suspected = true;
                        for (const cb of suspectCbs) cb();
                    }
                });
            }, heartbeatMs);
            (beat as { unref?: () => void }).unref?.();
            poll = setInterval(() => void refresh().catch(noop), pollMs);
            (poll as { unref?: () => void }).unref?.();
        },
        async setStatus(status: SiloStatus) {
            if (!self) return;
            self = { ...self, status };
            await writeSelf();
            await bumpVersion();
        },
        async leave() {
            if (beat) clearInterval(beat);
            if (poll) clearInterval(poll);
            beat = poll = null;
            if (!self) return;
            const id = self.siloId;
            self = null;
            await client.multi().del(siloKey(id)).srem(setKey, id).incr(mverKey).exec();
        },
        view: () => cached,
        refresh,
        async isAlive(siloId) {
            return (await client.exists(siloKey(siloId))) === 1;
        },
        onChange(cb) {
            changeCbs.add(cb);
            return () => changeCbs.delete(cb);
        },
        onSelfSuspect(cb) {
            suspectCbs.add(cb);
            return () => suspectCbs.delete(cb);
        }
    };
}

// ---------------------------------------------------------------------------
// Directory

/** Compare-and-delete: remove the key only if it holds the expected value. */
const COMPARE_DEL = `
local cur = redis.call('GET', KEYS[1])
if cur == ARGV[1] then redis.call('DEL', KEYS[1]); return 1 end
return 0
`;

/** Delete the key only if its value is owned by the silo (prefix match on
 *  the "siloId\n" value format) — the evictSilo sweep primitive. */
const DEL_IF_OWNER = `
local cur = redis.call('GET', KEYS[1])
if cur and string.sub(cur, 1, string.len(ARGV[1])) == ARGV[1] then
  redis.call('DEL', KEYS[1]); return 1
end
return 0
`;

const entryValue = (entry: DirectoryEntry): string =>
    `${entry.siloId}\n${entry.activationId}`;

function parseEntry(value: string): DirectoryEntry | null {
    const nl = value.indexOf('\n');
    if (nl <= 0) return null;
    return { siloId: value.slice(0, nl), activationId: value.slice(nl + 1) };
}

export function redisDirectory(
    client: RedisClient,
    options: { namespace?: string } = {}
): ActorDirectory {
    const ns = options.namespace ?? 'sigx';
    // actorId embeds a NUL — Redis keys are binary-safe, no escaping layer.
    const dirKey = (actorId: string): string => `${ns}:dir:${actorId}`;

    return {
        async lookup(actorId) {
            const value = await client.get(dirKey(actorId));
            return value === null ? null : parseEntry(value);
        },
        async claim(actorId, mine) {
            // SET NX GET (Redis ≥ 7): null = we won; otherwise the winner.
            const prior = await client.set(dirKey(actorId), entryValue(mine), 'NX', 'GET');
            if (prior === null) return mine;
            return parseEntry(prior) ?? mine;
        },
        async release(actorId, expected) {
            await client.eval(COMPARE_DEL, 1, dirKey(actorId), entryValue(expected));
        },
        async evict(actorId, expected) {
            const removed = await client.eval(COMPARE_DEL, 1, dirKey(actorId), entryValue(expected));
            return removed === 1;
        },
        async evictSilo(siloId) {
            // Cursor SCAN over the directory prefix + per-key owner-checked
            // delete. Assumes a single logical Redis (the demo/default
            // deployment); on Redis Cluster, run against each node.
            const prefix = `${siloId}\n`;
            let cursor = '0';
            let removed = 0;
            do {
                const [next, keys] = await client.scan(
                    cursor,
                    'MATCH',
                    `${ns}:dir:*`,
                    'COUNT',
                    500
                );
                cursor = next;
                for (const key of keys) {
                    removed += (await client.eval(DEL_IF_OWNER, 1, key, prefix)) === 1 ? 1 : 0;
                }
            } while (cursor !== '0');
            return removed;
        }
    };
}
