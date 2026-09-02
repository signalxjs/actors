/**
 * `@sigx/actors-redis` — Redis-backed cluster providers for
 * `@sigx/actors/cluster`: TTL-heartbeat membership and the claim
 * directory. Redis ≥ 7; ioredis is a peer dependency (the compare-ops are
 * Lua scripts, which need a concrete client API).
 *
 * Key layout under `{namespace}` (default `sigx`):
 *
 *   {ns}:host:{hostId}    HASH {d: descriptor JSON}   PX ttlMs, renewed each beat
 *   {ns}:hosts            SET of hostIds              lazily pruned on refresh
 *   {ns}:mver             INCR'd version counter      cheap poll compare
 *   {ns}:dir:{actorId}    "hostId\nactivationId"      no TTL — validity is the
 *                                                     owner's liveness
 *   {ns}:membership       pub/sub channel             membership-change push
 *                                                     (poll is the fallback)
 *
 * Directory entries carry no TTL by design: one heartbeat per host, not per
 * activation. The storage etag CAS in the actor runtime remains the
 * integrity floor underneath all of this.
 */
import { Redis } from 'ioredis';
import {
    heartbeatClock,
    refreshCoalescer,
    type ActorDirectory,
    type ClusterMembership,
    type ClusterProviders,
    type DirectoryEntry,
    type MembershipView,
    type HostDescriptor,
    type HostStatus
} from '@sigx/actors/cluster';

/** The subset of ioredis this package calls — accepts a shared app client. */
export type RedisClient = Redis;

export interface RedisClusterOptions {
    /** An existing ioredis client (shared with the app). */
    client?: RedisClient;
    /**
     * Or a URL — the package constructs its own client, with
     * `enableAutoPipelining: true` (#311). Safe here: the membership
     * subscriber is a `duplicate()` that only ever subscribes, and ioredis
     * never auto-pipelines subscription commands. Pass `client` instead to
     * own the options.
     */
    url?: string;
    /** Key namespace. Default `sigx`. */
    namespace?: string;
    /** Heartbeat cadence, ms. Default 5000. */
    heartbeatMs?: number;
    /** Heartbeat key TTL, ms (missed beats past this = dead). Default 15000. */
    ttlMs?: number;
    /** Membership view poll cadence, ms. Default 5000. */
    pollMs?: number;
    /**
     * Trailing quiet window for coalescing push notifications, ms. The
     * subscriber is single-flight either way (a burst of N changes costs
     * one refresh plus at most one trailing catch-up, not N); a non-zero
     * window widens the net past one round-trip at the price of that much
     * extra staleness. Default 0.
     */
    coalesceMs?: number;
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
        (options.url !== undefined
            ? new Redis(options.url, { enableAutoPipelining: true })
            : undefined);
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

/** Membership + directory for ONE host. */
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
    const { ns, heartbeatMs, ttlMs, pollMs, coalesceMs } = {
        ns: options.namespace ?? 'sigx',
        heartbeatMs: options.heartbeatMs ?? 5_000,
        ttlMs: options.ttlMs ?? 15_000,
        pollMs: options.pollMs ?? 5_000,
        coalesceMs: options.coalesceMs ?? 0
    };
    const setKey = `${ns}:hosts`;
    const mverKey = `${ns}:mver`;
    const channel = `${ns}:membership`;
    const hostKey = (id: string): string => `${ns}:host:${id}`;

    let self: HostDescriptor | null = null;
    let cached: MembershipView = { version: 0, hosts: [] };
    let beat: ReturnType<typeof setInterval> | null = null;
    let poll: ReturnType<typeof setInterval> | null = null;
    let subscriber: RedisClient | null = null;
    /** Heartbeat writes still on the wire. `leave()` drains them before its
     *  DEL: `clearInterval` stops FUTURE ticks, but a write already
     *  handed to the connection is neither awaited nor ordered against the
     *  DEL — it can commit AFTER it, recreating the host key until its TTL
     *  lapses (#209). */
    const inflight = new Set<Promise<unknown>>();
    /** Set by `leave()`: a beat completing after it began must not confirm
     *  the clock, and nothing may issue a new write. */
    let left = false;
    /** `mver` as last read — the pub/sub skip gate. Held apart from
     *  `cached.version`, which may run AHEAD of it: see `refresh`. */
    let storeVersion = 0;
    /** Host set + descriptor signature of the last view — see `refresh`. */
    let signature = '';
    const changeCbs = new Set<(view: MembershipView) => void>();
    const suspectCbs = new Set<() => void>();
    const clock = heartbeatClock({
        ttlMs,
        onSuspect: () => {
            for (const cb of suspectCbs) cb();
        }
    });

    const writeSelf = async (): Promise<void> => {
        if (!self || left) return;
        const write = client
            .multi()
            .hset(hostKey(self.hostId), 'd', JSON.stringify(self))
            .pexpire(hostKey(self.hostId), ttlMs)
            // Re-join the set on EVERY beat, not just at join: `refresh`
            // lazily `srem`s a member whose host key expired, so a host that
            // was pruned while it was away would otherwise keep heartbeating
            // into a set it is no longer in — invisible to every peer's view
            // forever (#45). SADD on an existing member is a no-op, and
            // folding it into the MULTI leaves no window where the host key
            // exists without its set entry. It can race `refresh`'s prune;
            // both converge within one beat.
            .sadd(setKey, self.hostId)
            .exec();
        inflight.add(write);
        try {
            await write;
        } finally {
            inflight.delete(write);
        }
        // `leave()` began while this was on the wire: the key is about to
        // go, so the clock must not read this as a live confirmation.
        if (left) return;
        clock.confirmed();
    };

    // Bump the version counter, then push: subscribers refresh immediately
    // instead of waiting out a poll interval. The poll stays as the safety
    // net for missed messages.
    const bumpVersion = async (): Promise<void> => {
        const version = await client.incr(mverKey);
        await client.publish(channel, String(version)).catch(noop);
    };

    const refresh = async (): Promise<MembershipView> => {
        const [verRaw, ids] = await Promise.all([client.get(mverKey), client.smembers(setKey)]);
        const pipeline = client.pipeline();
        for (const id of ids) pipeline.hget(hostKey(id), 'd');
        const rows = (await pipeline.exec()) ?? [];
        const hosts: HostDescriptor[] = [];
        const dead: string[] = [];
        rows.forEach(([err, value], i) => {
            if (err || typeof value !== 'string') {
                dead.push(ids[i]!);
                return;
            }
            try {
                hosts.push(JSON.parse(value) as HostDescriptor);
            } catch {
                dead.push(ids[i]!);
            }
        });
        if (dead.length > 0) {
            // Lazy prune: expired heartbeats leave set members behind.
            await client.srem(setKey, ...dead).catch(noop);
        }
        const stored = Number(verRaw ?? 0);
        // Signature, like the pg and surreal providers: a host REJOINING the
        // set (its beat re-`sadd`ing after a prune) writes no counter bump,
        // and an expiry never did either. Without this, `cached` would
        // silently gain or lose a host while `onChange` stayed quiet —
        // leaving transports unnotified and the departed-host sweep un-run.
        const nextSignature = hosts
            .map((h) => `${h.hostId}:${h.status}`)
            .sort()
            .join(',');
        const changed = stored !== storeVersion || nextSignature !== signature;
        // Unchanged: hand back the SAME object, so identity-keyed derived
        // data (`membersMemo()`, placement's caches) holds across polls.
        if (!changed) return cached;
        // The exposed version is a per-PROCESS change token, not `mver`: an
        // expiry (or that rejoin) changes `hosts` with no writer to bump
        // anything, so every observed change advances it — past the counter
        // when it must — and the two re-align only once written bumps carry
        // the counter past it (#267). A consumer keyed on `version`
        // therefore sees the expiry.
        const next: MembershipView = { version: Math.max(stored, cached.version + 1), hosts };
        storeVersion = stored;
        cached = next;
        signature = nextSignature;
        for (const cb of changeCbs) cb(next);
        return next;
    };

    // Coalesce the notification→refresh path (#26): a burst of pub/sub
    // messages costs one refresh plus at most one trailing catch-up per
    // subscriber, not one refresh per message — and a message whose
    // published counter the view has already caught up past costs nothing.
    // The gate is judged against the STORE's counter: the exposed version
    // may already be past it, and would wrongly skip a real bump.
    const coalescer = refreshCoalescer<MembershipView>({
        refresh,
        version: () => storeVersion,
        quietMs: coalesceMs
    });

    return {
        async join(descriptor) {
            left = false;
            self = descriptor;
            await writeSelf(); // …which `sadd`s us into the set as well
            await bumpVersion();
            await refresh();
            // Stamped HERE, not at the write above: `bumpVersion` and a full
            // O(N) `refresh` sit between them and can outlast `ttlMs` on a
            // large or slow store, which would make the first beat late by
            // construction — a terminal fence on every host at startup.
            clock.arm();
            beat = setInterval(() => {
                // Before the write, deliberately: if the window lapsed our
                // claims are already gone, and waiting out a store
                // round-trip only lets a doomed activation take more turns.
                clock.beat();
                void writeSelf().catch(() => clock.failed());
            }, heartbeatMs);
            (beat as { unref?: () => void }).unref?.();
            // The poll goes through the coalescer too, so a poll tick landing
            // during a push-triggered refresh joins it instead of double-reading.
            poll = setInterval(() => void coalescer.demand().catch(noop), pollMs);
            (poll as { unref?: () => void }).unref?.();
            // Push: a dedicated subscriber connection (ioredis subscriber
            // mode can't run other commands) hints the coalescer on publish.
            // The payload is the published version — the skip gate; a
            // non-numeric payload still refreshes, just unskippably.
            subscriber = client.duplicate();
            subscriber.on('message', (_channel: string, message: string) => {
                const noted = Number(message);
                coalescer.note(Number.isFinite(noted) && noted > 0 ? noted : undefined);
            });
            await subscriber.subscribe(channel).catch(noop);
        },
        async setStatus(status: HostStatus) {
            if (!self) return;
            self = { ...self, status };
            await writeSelf();
            await bumpVersion();
        },
        async leave() {
            left = true;
            if (beat) clearInterval(beat);
            if (poll) clearInterval(poll);
            beat = poll = null;
            if (subscriber) {
                // Stop new hints FIRST — under churn, a still-attached
                // handler could keep scheduling refreshes and hold
                // `settled()` open indefinitely — then drain what already
                // started (refreshes read via `client`, not the subscriber).
                subscriber.removeAllListeners('message');
                await coalescer.settled();
                await subscriber.quit().catch(noop);
                subscriber = null;
            }
            if (!self) return;
            const id = self.hostId;
            self = null;
            // A beat already on the wire must land BEFORE the DEL, or it
            // lands after and the host key comes back (#209).
            await Promise.allSettled(inflight);
            // Removal and version bump commit atomically; the push is
            // best-effort on top (the poll covers a lost publish).
            const results = await client
                .multi()
                .del(hostKey(id))
                .srem(setKey, id)
                .incr(mverKey)
                .exec();
            const version = results?.[2]?.[1];
            await client.publish(channel, String(version ?? '')).catch(noop);
        },
        view: () => cached,
        // Demand semantics: resolves with a refresh that started at-or-after
        // the call — placement's failure path relies on the refreshed view
        // excluding a leaver it just observed.
        refresh: () => coalescer.demand(),
        async isAlive(hostId) {
            return (await client.exists(hostKey(hostId))) === 1;
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

/** Delete the key only if its value is owned by the host (prefix match on
 *  the "hostId\n" value format) — the evictHost sweep primitive. */
const DEL_IF_OWNER = `
local cur = redis.call('GET', KEYS[1])
if cur and string.sub(cur, 1, string.len(ARGV[1])) == ARGV[1] then
  redis.call('DEL', KEYS[1]); return 1
end
return 0
`;

const entryValue = (entry: DirectoryEntry): string =>
    `${entry.hostId}\n${entry.activationId}`;

function parseEntry(value: string): DirectoryEntry | null {
    const nl = value.indexOf('\n');
    if (nl <= 0) return null;
    return { hostId: value.slice(0, nl), activationId: value.slice(nl + 1) };
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
        async evictHost(hostId) {
            // Cursor SCAN over the directory prefix + per-key owner-checked
            // delete. Assumes a single logical Redis (the demo/default
            // deployment); on Redis Cluster, run against each node.
            const prefix = `${hostId}\n`;
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

// ---------------------------------------------------------------------------
// Storage

export { redisStorage, type RedisStorageOptions } from './storage';
