/**
 * redisReminders — the Redis-specific mechanics (#385). The contract
 * itself is `remindersConformance` (`reminders-conformance.test.ts`); this
 * file pins what only this provider decides: the member escaping, the key
 * layout, that `ownsShard` is ignored (every host claims from one sorted
 * set), the lowered tick a cheap empty claim allows, and the end-to-end
 * wiring through `createHost`.
 *
 * The escaping cases are pure and run everywhere; the rest is env-gated on
 * `REDIS_URL`.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { Redis } from 'ioredis';
import { defineActor, type ActorRef } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage, type Host } from '@sigx/actors/host';
import { redisReminders, redisStorage, remEscape, remUnescape } from '@sigx/actors-redis';

const REDIS_URL = process.env.REDIS_URL;
const NUL = '\u0000';

describe('remEscape / remUnescape', () => {
    it('round-trips NUL, backslashes and their lookalikes, and never emits a NUL', () => {
        const samples = ['', 'plain', `a${NUL}b`, 'a\\b', `a\\${NUL}b`, '\\0', `${NUL}${NUL}`, '\\\\0', `end${NUL}`];
        for (const sample of samples) {
            const escaped = remEscape(sample);
            expect(escaped).not.toContain(NUL);
            expect(remUnescape(escaped)).toBe(sample);
        }
    });

    it('is injective over inputs that collide un-escaped', () => {
        // `\0` (two characters) and a real NUL must not meet.
        expect(remEscape('\\0')).not.toBe(remEscape(NUL));
        expect(remEscape('a\\')).not.toBe(remEscape(`a${NUL}`));
    });
});

describe.skipIf(!REDIS_URL)('redisReminders', () => {
    const clients: Redis[] = [];
    const namespaces: string[] = [];
    let running: Host | null = null;

    function fresh(): { client: Redis; namespace: string } {
        const client = new Redis(REDIS_URL!);
        const namespace = `sigx-remt-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
        clients.push(client);
        namespaces.push(namespace);
        return { client, namespace };
    }

    afterEach(async () => {
        await running?.stop({ timeoutMs: 1000 });
        running = null;
        for (const [i, client] of clients.entries()) {
            let cursor = '0';
            do {
                const [next, keys] = await client.scan(cursor, 'MATCH', `${namespaces[i]}*`, 'COUNT', 500);
                cursor = next;
                if (keys.length) await client.del(...keys);
            } while (cursor !== '0');
            await client.quit();
        }
        clients.length = 0;
        namespaces.length = 0;
    });

    it('keeps one sorted set, one period hash and one name set per actor — and clears them', async () => {
        const { client, namespace } = fresh();
        const provider = redisReminders({ client, namespace });
        provider.bind({
            storage: memoryStorage(),
            scheduler: manualScheduler(),
            tickMs: 1000,
            ownsShard: () => true,
            deliver: async () => {}
        });
        const ref: ActorRef = { type: 'Room', key: `k${NUL}1` };
        const api = provider.apiFor(ref);
        await api.set('once', { due: 3_600_000 });
        await api.set('beat', { due: 3_600_000, period: 60_000 });
        const due = `${namespace}:rem:due`;
        const periods = `${namespace}:rem:p`;
        const actor = `${namespace}:rem:a:${remEscape(ref.type)}${NUL}${remEscape(ref.key)}`;
        expect(await client.zcard(due)).toBe(2);
        // Only the periodic member carries a period.
        expect(await client.hlen(periods)).toBe(1);
        expect((await client.smembers(actor)).sort()).toEqual(['beat', 'once']);
        // The score is the server clock plus the delay, in ms.
        const [, score] = await client.zrange(due, 0, 0, 'WITHSCORES');
        expect(Number(score)).toBeGreaterThan(Date.now() + 3_500_000);
        expect(Number(score)).toBeLessThan(Date.now() + 3_700_000);
        await api.clear('once');
        await api.clear('beat');
        expect(await client.zcard(due)).toBe(0);
        expect(await client.hlen(periods)).toBe(0);
        expect(await client.exists(actor)).toBe(0);
    });

    it('ignores ownsShard: a host that owns no shard still claims (every host ticks one set)', async () => {
        const { client, namespace } = fresh();
        const provider = redisReminders({ client, namespace });
        const scheduler = manualScheduler();
        const delivered: string[] = [];
        provider.bind({
            storage: memoryStorage(),
            scheduler,
            tickMs: 1000,
            ownsShard: () => false,
            deliver: async (_ref, name) => void delivered.push(name)
        });
        await provider.apiFor({ type: 'T', key: 'k' }).set('wake', { due: 0 });
        provider.start();
        scheduler.advance(1000);
        await vi.waitFor(() => expect(delivered).toEqual(['wake']));
        provider.stop();
    });

    it('claims a batch at a time and drains a backlog within one tick', async () => {
        const { client, namespace } = fresh();
        const provider = redisReminders({ client, namespace, batchSize: 5 });
        const scheduler = manualScheduler();
        let delivered = 0;
        provider.bind({
            storage: memoryStorage(),
            scheduler,
            tickMs: 1000,
            ownsShard: () => true,
            deliver: async () => void delivered++
        });
        for (let i = 0; i < 23; i++) {
            await provider.apiFor({ type: 'T', key: `k${i}` }).set('wake', { due: 0 });
        }
        provider.start();
        scheduler.advance(1000);
        // 23 due at batch 5: five claims in one tick, not one per tick.
        await vi.waitFor(() => expect(delivered).toBe(23));
        provider.stop();
    });

    it('refuses to bind twice — one provider per host', () => {
        const { client, namespace } = fresh();
        const provider = redisReminders({ client, namespace });
        const context = {
            storage: memoryStorage(),
            scheduler: manualScheduler(),
            tickMs: 1000,
            ownsShard: () => true,
            deliver: async () => {}
        };
        provider.bind(context);
        expect(() => provider.bind(context)).toThrow(/already bound/);
    });

    it('end to end: a real actor’s onReminder fires through createHost, at a 200 ms tick', async () => {
        const { client, namespace } = fresh();
        const fired: string[] = [];
        const Pinger = defineActor({
            type: 'Pinger',
            allowAnonymous: true,
            state: () => ({ armed: false }),
            methods: (ctx) => ({
                async arm() {
                    await ctx.reminders.set('ping', { due: 0 });
                    ctx.state.armed = true;
                },
                list: () => ctx.reminders.list()
            }),
            onReminder: (_ctx, name) => {
                fired.push(name);
            }
        });
        const scheduler = manualScheduler();
        const host = createHost({
            actors: [Pinger],
            storage: redisStorage({ client, namespace }),
            reminders: redisReminders({ client, namespace }),
            scheduler,
            // The point of the index: a tick this short costs one empty
            // ZRANGEBYSCORE, where the sharded table would load every shard.
            defaults: { sweepIntervalMs: 0, reminderTickMs: 200, callTimeoutMs: 0 }
        });
        running = host;
        await host.start();
        await host.actor(Pinger, 'p1').arm();
        scheduler.advance(200);
        await vi.waitFor(() => expect(fired).toEqual(['ping']));
        await vi.waitFor(async () => expect(await host.actor(Pinger, 'p1').list()).toEqual([]));
    });
});

describe.runIf(!REDIS_URL)('redisReminders (no REDIS_URL)', () => {
    it('skips the Redis suite when REDIS_URL is not set', () => {
        expect(REDIS_URL).toBeUndefined();
    });
});
