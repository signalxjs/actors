/**
 * #209 — `leave()` must not race its own heartbeat. The beat fires
 * `void writeSelf()` every `heartbeatMs` without tracking the promise;
 * `clearInterval` stops FUTURE ticks, but a MULTI already on the wire is
 * neither awaited nor ordered against the `DEL` that follows — on a
 * pipelined or slow connection it commits AFTER it, recreating the host
 * key (and re-`SADD`ing the member) until its TTL lapses.
 *
 * Not env-gated: the client is a fake whose heartbeat MULTI commits when
 * the test says so. It is typed as ioredis's `Redis` only because the
 * seam is; membership touches a handful of commands and this models them.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { redisMembership, type RedisClient } from '../src';

type Reply = [Error | null, unknown];

interface RacingRedis {
    client: RedisClient;
    /** Host keys present — a heartbeat lands when its MULTI COMMITS. */
    hosts: Map<string, string>;
    members: Set<string>;
    /** Every write, in commit order. */
    log: string[];
    /** Hold the next MULTI on the wire until `commitHeld()`. */
    holdNextMulti(): void;
    held(): boolean;
    commitHeld(): void;
}

/** The commands membership uses, over a host map and a member set. */
function racingRedis(): RacingRedis {
    const hosts = new Map<string, string>();
    const members = new Set<string>();
    const log: string[] = [];
    let version = 0;
    let holdNext = false;
    let held: (() => void) | null = null;

    const multi = (): unknown => {
        const ops: Array<() => unknown> = [];
        const chain = {
            hset(key: string, _field: string, value: string) {
                ops.push(() => {
                    hosts.set(key, value);
                    log.push(`hset ${key}`);
                    return 1;
                });
                return chain;
            },
            pexpire() {
                ops.push(() => 1);
                return chain;
            },
            sadd(_set: string, id: string) {
                ops.push(() => {
                    members.add(id);
                    return 1;
                });
                return chain;
            },
            del(key: string) {
                ops.push(() => {
                    hosts.delete(key);
                    log.push(`del ${key}`);
                    return 1;
                });
                return chain;
            },
            srem(_set: string, id: string) {
                ops.push(() => {
                    members.delete(id);
                    return 1;
                });
                return chain;
            },
            incr() {
                ops.push(() => ++version);
                return chain;
            },
            exec(): Promise<Reply[]> {
                const run = (): Reply[] => ops.map((op) => [null, op()]);
                if (holdNext) {
                    holdNext = false;
                    return new Promise((resolve) => {
                        held = () => resolve(run());
                    });
                }
                return Promise.resolve(run());
            }
        };
        return chain;
    };

    const client = {
        multi,
        pipeline() {
            const keys: string[] = [];
            const chain = {
                hget(key: string) {
                    keys.push(key);
                    return chain;
                },
                exec: (): Promise<Reply[]> =>
                    Promise.resolve(keys.map((key) => [null, hosts.get(key) ?? null]))
            };
            return chain;
        },
        incr: () => Promise.resolve(++version),
        publish: () => Promise.resolve(0),
        get: () => Promise.resolve(String(version)),
        smembers: () => Promise.resolve([...members]),
        srem: (_set: string, ...ids: string[]) => {
            for (const id of ids) members.delete(id);
            return Promise.resolve(ids.length);
        },
        exists: (key: string) => Promise.resolve(hosts.has(key) ? 1 : 0),
        duplicate: () => ({
            on() {},
            removeAllListeners() {},
            subscribe: () => Promise.resolve(1),
            quit: () => Promise.resolve('OK')
        })
    } as unknown as RedisClient;

    return {
        client,
        hosts,
        members,
        log,
        holdNextMulti: () => {
            holdNext = true;
        },
        held: () => held !== null,
        commitHeld: () => {
            const commit = held;
            held = null;
            commit?.();
        }
    };
}

/** Enough microtask turns for `leave()` to reach its DEL unimpeded. */
const flush = async (): Promise<void> => {
    for (let i = 0; i < 20; i++) await Promise.resolve();
};

afterEach(() => {
    vi.useRealTimers();
});

describe('redisMembership leave vs in-flight heartbeat (#209)', () => {
    it('leave() waits for a heartbeat already on the wire before its DEL', async () => {
        vi.useFakeTimers();
        const r = racingRedis();
        const m = redisMembership(r.client, {
            namespace: 't',
            heartbeatMs: 50,
            ttlMs: 200,
            pollMs: 60_000
        });
        await m.join({ hostId: 'h1', epoch: 1, address: 'http://h1', status: 'active' });
        expect(r.hosts.has('t:host:h1')).toBe(true);

        // The next beat's MULTI is on the wire but has not committed yet.
        r.holdNextMulti();
        await vi.advanceTimersByTimeAsync(50);
        expect(r.held()).toBe(true);

        const leaving = m.leave();
        try {
            await flush();
            // The DEL must wait for that MULTI: issued now, it would be
            // overtaken by the commit and the host key would come back.
            expect(r.log).not.toContain('del t:host:h1');
        } finally {
            r.commitHeld();
        }
        await leaving;
        expect(r.log).toEqual(['hset t:host:h1', 'hset t:host:h1', 'del t:host:h1']);
        expect(r.hosts.has('t:host:h1')).toBe(false);
        expect(r.members.has('h1')).toBe(false);
    });
});
