/**
 * `redisReminders` — durable reminders on a due-time-indexed sorted set,
 * replacing the default sharded scan for deployments whose one store is
 * Redis (#385). The default `shardedReminders()` keeps 16 fixed shard
 * records in `ActorStorage`, and every `set` loads one, stringifies it
 * twice and CAS-saves it back, while every tick loads and scans the whole
 * owned record: honest work at dev scale and O(table) at "many reminders"
 * — `BASELINES.md` (2026-09-04) measured a `set` at 870 ms p50 with
 * 100 000 entries asleep, and a quarter of fires landing within a tick.
 * Here an arm is O(log N), a tick is O(due), and an empty claim is one
 * `ZRANGEBYSCORE` that returns nothing — cheap enough that
 * `reminderTickMs` can drop from the runtime's 30 s default to a few
 * seconds, which is what turns a durable wake's lag from "half a tick" of
 * fifteen seconds into one of two or three.
 *
 * The layout, under the shared `{namespace}` prefix:
 *
 *   {ns}:rem:due                 ZSET  score = next_due (epoch ms), member
 *   {ns}:rem:p                   HASH  member → period_ms (periodic only)
 *   {ns}:rem:a:{type}<NUL>{key}  SET   the actor's reminder names, for list()
 *
 * A member is `type<NUL>key<NUL>name` with each field ESCAPED (`\` → `\\`,
 * NUL → `\0`), so the claim can split it back into a ref and a name however
 * many NULs the raw strings carry; the actor-set key and the names it
 * holds use the same escaping, so a script can derive the set from a
 * member without a lookup. `list()` is one `SMEMBERS` — never a scan.
 *
 * Every operation is ONE Lua script and the script's atomicity IS the
 * transaction (`redisStorage` follows the same rule):
 *
 * - **At-most-once per firing**: the claim advances or removes the member
 *   BEFORE any dispatch, so a crash between claim and dispatch skips one
 *   firing rather than double-firing — the same posture as every other
 *   provider.
 * - **No shard ownership needed**: a script runs alone, so every host may
 *   claim from the one sorted set concurrently and each gets disjoint
 *   members — the `SKIP LOCKED` posture of `pgReminders`, and
 *   `ActorRemindersContext.ownsShard` is deliberately ignored.
 * - **No catch-up bursts**: a periodic member advances to
 *   `max(next_due, now) + period` — after downtime it fires once and
 *   resumes its cadence, never replaying the gap.
 * - **Server clock throughout**: `due` is applied to `TIME` inside the
 *   script, so host clock skew cannot fire early or late.
 *
 * A dispatch that FAILS is not a firing (#306, #326). The claim has already
 * advanced or removed the member, so a rejected `deliver()` — a deadline,
 * a host mid-restart, an `onReminder` that threw — would otherwise be the
 * end of the wake. Instead the member is re-armed one tick out on the
 * server clock (`next_due = now + tickMs`: a one-shot re-inserted, a
 * periodic one pulled forward) in ONE script per batch, and each failure
 * is reported through `context.undelivered`. Same rules as the sharded
 * table: a member the actor SET again meanwhile is left as the actor set
 * it (a later decision wins — the re-arm compares the score against the
 * one the claim wrote), a periodic one it CLEARED stays cleared, and a
 * one-shot it cleared may be retried once — the claim had already removed
 * it, so absent is indistinguishable from untouched.
 *
 * Delivery failures are `allSettled` — one bad reminder cannot kill the
 * loop — and the loop is single-flight per provider instance. (A Redis
 * Cluster deployment would need the three keys in one hash slot; the key
 * layout carries no hash tag today, as `redisStorage`'s does not either.)
 */
import { Redis } from 'ioredis';
import type { ActorReminders, ActorRemindersContext, ActorRef, ReminderApi } from '@sigx/actors';
import type { RedisClient } from './index';

export interface RedisRemindersOptions {
    /** An existing ioredis client (shared with the other providers). */
    client?: RedisClient;
    /** Or a URL — the package constructs its own client, auto-pipelined. */
    url?: string;
    /** Key namespace. Default `sigx`. */
    namespace?: string;
    /** Members claimed per script. A tick keeps claiming until a batch
     *  comes back short (bounded), so this caps memory, not throughput.
     *  Default 200. */
    batchSize?: number;
}

/** Same floor as the default provider: anything tighter is a timer's job. */
const MIN_PERIOD_MS = 60_000;
/** Claim batches per tick — a backlog drains fast without an unbounded
 *  loop if members keep becoming due mid-tick. */
const MAX_BATCHES_PER_TICK = 10;

const NUL = '\u0000';

/** Injective, NUL-free: `\` → `\\`, NUL → `\0`. */
export function remEscape(field: string): string {
    return field.replace(/\\/g, '\\\\').split(NUL).join('\\0');
}

export function remUnescape(field: string): string {
    return field.replace(/\\(\\|0)/g, (_m, c: string) => (c === '0' ? NUL : '\\'));
}

/** Server time in ms, then the ZADD that `set` and `rearm` share. */
const LUA_NOW = `
local t = redis.call('TIME')
local now = tonumber(t[1]) * 1000 + math.floor(tonumber(t[2]) / 1000)
`;

/** The actor-set key and escaped name a member encodes. */
const LUA_SPLIT = `
local function split(m)
  local cut = nil
  local pos = 1
  while true do
    local f = string.find(m, string.char(0), pos, true)
    if not f then break end
    cut = f
    pos = f + 1
  end
  return string.sub(m, 1, cut - 1), string.sub(m, cut + 1)
end
`;

/**
 * set. KEYS[1] due, KEYS[2] periods, KEYS[3] the actor set. ARGV[1] member,
 * ARGV[2] delay ms, ARGV[3] period ms or '', ARGV[4] the escaped name.
 */
const SET = `${LUA_NOW}
redis.call('ZADD', KEYS[1], string.format('%.0f', now + tonumber(ARGV[2])), ARGV[1])
if ARGV[3] ~= '' then
  redis.call('HSET', KEYS[2], ARGV[1], ARGV[3])
else
  redis.call('HDEL', KEYS[2], ARGV[1])
end
redis.call('SADD', KEYS[3], ARGV[4])
return 1
`;

/** clear. Same keys; ARGV[1] member, ARGV[2] the escaped name. */
const CLEAR = `
redis.call('ZREM', KEYS[1], ARGV[1])
redis.call('HDEL', KEYS[2], ARGV[1])
redis.call('SREM', KEYS[3], ARGV[2])
return 1
`;

/**
 * claim. KEYS[1] due, KEYS[2] periods. ARGV[1] batch, ARGV[2] the actor-set
 * prefix. Returns a flat list of `member, advanced` pairs — `advanced` the
 * score written for a periodic member, '' for a removed one-shot.
 */
const CLAIM = `${LUA_NOW}${LUA_SPLIT}
local due = redis.call('ZRANGEBYSCORE', KEYS[1], '-inf', now, 'WITHSCORES', 'LIMIT', 0, tonumber(ARGV[1]))
local out = {}
for i = 1, #due, 2 do
  local m = due[i]
  local score = tonumber(due[i + 1])
  local p = redis.call('HGET', KEYS[2], m)
  if p then
    local advanced = string.format('%.0f', math.max(score, now) + tonumber(p))
    redis.call('ZADD', KEYS[1], advanced, m)
    out[#out + 1] = m
    out[#out + 1] = advanced
  else
    redis.call('ZREM', KEYS[1], m)
    local actor, name = split(m)
    redis.call('SREM', ARGV[2] .. actor, name)
    out[#out + 1] = m
    out[#out + 1] = ''
  end
end
return out
`;

/**
 * rearm. KEYS[1] due, KEYS[2] periods. ARGV[1] tick ms, ARGV[2] the
 * actor-set prefix, then `member, advanced` pairs as the claim returned
 * them. A one-shot is re-inserted only if absent (the actor set it again
 * meanwhile, or — the documented exception — cleared it); a periodic one is
 * pulled forward only while its score is still the one the claim wrote.
 */
const REARM = `${LUA_NOW}${LUA_SPLIT}
local retry = now + tonumber(ARGV[1])
for i = 3, #ARGV, 2 do
  local m = ARGV[i]
  local advanced = ARGV[i + 1]
  local cur = redis.call('ZSCORE', KEYS[1], m)
  if advanced == '' then
    if not cur then
      redis.call('ZADD', KEYS[1], string.format('%.0f', retry), m)
      local actor, name = split(m)
      redis.call('SADD', ARGV[2] .. actor, name)
    end
  elseif cur and tonumber(cur) == tonumber(advanced) then
    redis.call('ZADD', KEYS[1], string.format('%.0f', math.min(tonumber(cur), retry)), m)
  end
end
return 1
`;

interface ReminderCommands {
    sigxRemSet(due: string, periods: string, actor: string, member: string, delay: string, period: string, name: string): Promise<number>;
    sigxRemClear(due: string, periods: string, actor: string, member: string, name: string): Promise<number>;
    sigxRemClaim(due: string, periods: string, batch: string, prefix: string): Promise<string[]>;
    sigxRemRearm(due: string, periods: string, ...args: string[]): Promise<number>;
}

/** A member the claim took; `advanced` is the score it wrote, '' for a
 *  removed one-shot. */
interface Claimed {
    member: string;
    advanced: string;
}

function decode(member: string): { ref: ActorRef; name: string } {
    const parts = member.split(NUL);
    // Escaped fields carry no NUL, so a member splits into exactly three.
    const [type = '', key = '', name = ''] = parts;
    return { ref: { type: remUnescape(type), key: remUnescape(key) }, name: remUnescape(name) };
}

export function redisReminders(options: RedisRemindersOptions = {}): ActorReminders {
    const base =
        options.client ??
        (options.url !== undefined ? new Redis(options.url, { enableAutoPipelining: true }) : undefined);
    if (!base) {
        throw new Error('[sigx actors-redis] pass either `client` (ioredis) or `url`.');
    }
    const ns = options.namespace ?? 'sigx';
    const batchSize = Math.max(1, options.batchSize ?? 200);
    const dueKey = `${ns}:rem:due`;
    const periodKey = `${ns}:rem:p`;
    const actorPrefix = `${ns}:rem:a:`;
    const actorId = (ref: ActorRef): string => `${remEscape(ref.type)}${NUL}${remEscape(ref.key)}`;

    // Re-registering on a shared client is an overwrite, so several
    // providers (or one next to `redisStorage`) can share one connection.
    base.defineCommand('sigxRemSet', { numberOfKeys: 3, lua: SET });
    base.defineCommand('sigxRemClear', { numberOfKeys: 3, lua: CLEAR });
    base.defineCommand('sigxRemClaim', { numberOfKeys: 2, lua: CLAIM });
    base.defineCommand('sigxRemRearm', { numberOfKeys: 2, lua: REARM });
    const client = base as RedisClient & ReminderCommands;

    let ctx: ActorRemindersContext | null = null;
    let cancel: (() => void) | null = null;
    let ticking = false;
    let stopped = false;

    const claim = async (): Promise<Claimed[]> => {
        const flat = await client.sigxRemClaim(dueKey, periodKey, String(batchSize), actorPrefix);
        const out: Claimed[] = [];
        for (let i = 0; i + 1 < flat.length; i += 2) {
            out.push({ member: flat[i]!, advanced: flat[i + 1]! });
        }
        return out;
    };

    const rearm = async (failed: readonly Claimed[], tickMs: number): Promise<void> => {
        const args: string[] = [String(Math.max(0, tickMs)), actorPrefix];
        for (const row of failed) args.push(row.member, row.advanced);
        await client.sigxRemRearm(dueKey, periodKey, ...args);
    };

    const tick = async (): Promise<void> => {
        const context = ctx;
        if (!context || ticking || stopped) return;
        ticking = true;
        try {
            for (let batch = 0; batch < MAX_BATCHES_PER_TICK; batch++) {
                const due = await claim();
                if (due.length === 0) return;
                // The claim above is already durable; deliveries are
                // isolated so one failing actor cannot starve the rest, and
                // the batch's failures go back in ONE script once every
                // dispatch has settled (#326).
                const failed: Claimed[] = [];
                await Promise.allSettled(
                    due.map(async (row) => {
                        const { ref, name } = decode(row.member);
                        try {
                            // Awaited INSIDE the try: a `deliver` that throws
                            // before it returns a promise lands here exactly
                            // like a rejection.
                            await context.deliver(ref, name);
                        } catch (error) {
                            failed.push(row);
                            if (__DEV__) {
                                console.error(
                                    `[sigx actors-redis] reminder "${name}" on ${ref.type}/${ref.key} ` +
                                        'failed (retrying next tick):',
                                    error
                                );
                            }
                            try {
                                context.undelivered?.(ref, name, error);
                            } catch (reportError) {
                                if (__DEV__) {
                                    console.error(
                                        '[sigx actors-redis] ActorRemindersContext.undelivered threw:',
                                        reportError
                                    );
                                }
                            }
                        }
                    })
                );
                if (failed.length > 0) {
                    try {
                        await rearm(failed, context.tickMs);
                    } catch (error) {
                        // Redis blinked between the claim and now — those
                        // wakes ARE lost, and the counter above already says
                        // so. Do not fail the tick over it.
                        if (__DEV__) {
                            console.error(
                                `[sigx actors-redis] could not re-arm ${failed.length} failed reminder(s):`,
                                error
                            );
                        }
                    }
                }
                if (due.length < batchSize) return;
            }
        } finally {
            ticking = false;
        }
    };

    return {
        bind(context) {
            if (ctx) {
                throw new Error(
                    '[sigx actors-redis] redisReminders is already bound — create one provider per host.'
                );
            }
            ctx = context;
        },
        start() {
            const context = ctx;
            if (!context) {
                throw new Error('[sigx actors-redis] redisReminders.start() before bind().');
            }
            if (cancel) return; // idempotent — one interval, ever
            stopped = false;
            cancel = context.scheduler.every(context.tickMs, () =>
                // A failed claim (Redis blinked) must neither surface as an
                // unhandled rejection nor stop the loop.
                void tick().catch((error: unknown) => {
                    if (__DEV__) console.warn('[sigx actors-redis] reminder tick failed:', error);
                })
            );
        },
        stop() {
            stopped = true;
            cancel?.();
            cancel = null;
        },
        apiFor(ref): ReminderApi {
            const actor = actorId(ref);
            const actorKey = actorPrefix + actor;
            const memberOf = (name: string): string => `${actor}${NUL}${remEscape(name)}`;
            return {
                async set(name, opts) {
                    if (opts.period !== undefined && opts.period < MIN_PERIOD_MS) {
                        throw new Error(
                            `[sigx actors-redis] reminder "${name}" period ${opts.period}ms is under ` +
                                `the ${MIN_PERIOD_MS}ms floor — use ctx.timer for tighter cadences.`
                        );
                    }
                    await client.sigxRemSet(
                        dueKey,
                        periodKey,
                        actorKey,
                        memberOf(name),
                        String(Math.max(0, opts.due)),
                        opts.period === undefined ? '' : String(opts.period),
                        remEscape(name)
                    );
                },
                async clear(name) {
                    await client.sigxRemClear(dueKey, periodKey, actorKey, memberOf(name), remEscape(name));
                },
                async list() {
                    const names = await client.smembers(actorKey);
                    return names.map(remUnescape).sort();
                }
            };
        }
    };
}
