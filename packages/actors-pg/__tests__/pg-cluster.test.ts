/**
 * Postgres provider tests — env-gated on `PG_URL`. Covers the provider
 * contracts (claim CAS matrix, compare-and-delete, membership heartbeat +
 * database-clock expiry, LISTEN/NOTIFY push) and a 2-host end-to-end smoke
 * over real Postgres — storage, membership and directory all riding one
 * pool.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import pg from 'pg';
import { defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import {
    clusterPlacement,
    handleHostRequest,
    matchesHostRequest,
    type ClusterPlacement
} from '@sigx/actors/cluster';
import { ensurePgSchema, pgCluster, pgDirectory, pgMembership, pgStorage, pgText } from '@sigx/actors-pg';

const PG_URL = process.env.PG_URL;

describe.skipIf(!PG_URL)('pg cluster providers', () => {
    let pool: pg.Pool;
    let schema: string;

    beforeAll(async () => {
        pool = new pg.Pool({ connectionString: PG_URL });
        schema = `sigx_test_${Date.now().toString(36)}${Math.random().toString(36).slice(2, 6)}`;
        await ensurePgSchema(pool, { schema });
    });
    afterAll(async () => {
        await pool.query(`DROP SCHEMA IF EXISTS ${schema} CASCADE`);
        await pool.end();
    });

    describe('schema', () => {
        it('refuses an unsafe schema identifier everywhere it is taken', () => {
            expect(() => pgDirectory(pool, { schema: 'bad-name' })).toThrow(/identifier/);
            expect(() => pgMembership(pool, { schema: 'x; DROP TABLE t' })).toThrow(
                /identifier/
            );
            expect(() => pgStorage({ pool, schema: 'Sigx' })).toThrow(/identifier/);
        });
    });

    describe('directory', () => {
        it('claim is create-if-absent and returns the winner', async () => {
            const dir = pgDirectory(pool, { schema });
            const a = { hostId: 's.a', activationId: 's.a/1/1' };
            const b = { hostId: 's.b', activationId: 's.b/1/1' };
            await expect(dir.claim('Counter k', a)).resolves.toEqual(a);
            await expect(dir.claim('Counter k', b)).resolves.toEqual(a); // lost race → winner
            await expect(dir.lookup('Counter k')).resolves.toEqual(a);
        });

        it('a REAL actor id — type NUL key — claims and releases cleanly', async () => {
            // The runtime's directory keys are `type<NUL>key`, and Postgres
            // text rejects raw NUL: this pins the pgText escape on every
            // directory operation (the 2-host smoke exercises it end to end).
            const NUL = String.fromCharCode(0);
            const dir = pgDirectory(pool, { schema });
            const id = `Counter${NUL}shared`;
            const mine = { hostId: 's.n1', activationId: 's.n1/1/1' };
            await expect(dir.claim(id, mine)).resolves.toEqual(mine);
            await expect(dir.lookup(id)).resolves.toEqual(mine);
            // Injective: the visually-escaped id is a DIFFERENT actor.
            await expect(dir.lookup('Counter\\0shared')).resolves.toBeNull();
            await expect(dir.evict(id, mine)).resolves.toBe(true);
            await expect(dir.lookup(id)).resolves.toBeNull();
        });

        it('release and evict are compare-and-delete on the entry', async () => {
            const dir = pgDirectory(pool, { schema });
            const a = { hostId: 's.a', activationId: 's.a/1/1' };
            const stale = { hostId: 's.a', activationId: 's.a/0/9' };
            await dir.claim('T k', a);
            await dir.release('T k', stale); // wrong expected → no-op
            await expect(dir.lookup('T k')).resolves.toEqual(a);
            await expect(dir.evict('T k', a)).resolves.toBe(true);
            await expect(dir.lookup('T k')).resolves.toBeNull();
            await expect(dir.evict('T k', a)).resolves.toBe(false); // already gone
        });

        it('evictHost sweeps exactly the dead host’s entries', async () => {
            const dir = pgDirectory(pool, { schema });
            await dir.claim('E a', { hostId: 's.dead', activationId: 's.dead/1/1' });
            await dir.claim('E b', { hostId: 's.dead', activationId: 's.dead/1/2' });
            await dir.claim('E c', { hostId: 's.alive', activationId: 's.alive/1/1' });
            await expect(dir.evictHost!('s.dead')).resolves.toBe(2);
            await expect(dir.lookup('E a')).resolves.toBeNull();
            await expect(dir.lookup('E c')).resolves.toEqual({
                hostId: 's.alive',
                activationId: 's.alive/1/1'
            });
            await expect(dir.evictHost!('s.dead')).resolves.toBe(0); // idempotent
        });
    });

    describe('membership', () => {
        it('HostDescriptor.types round-trips join → view and survives setStatus (#214)', async () => {
            // Deep-equal on the WHOLE descriptor: a provider that projects
            // known fields instead of round-tripping would drop `types` and
            // silently make a heterogeneous cluster eligible-for-everything.
            const opts = { schema, heartbeatMs: 100, ttlMs: 400, pollMs: 100 };
            const m1 = pgMembership(pool, opts);
            const m2 = pgMembership(pool, opts);
            const typed = {
                hostId: 's.typed',
                epoch: 7,
                address: 'http://typed',
                status: 'active' as const,
                types: ['Counter', 'Relay']
            };
            await m1.join(typed);
            await m2.join({ hostId: 's.plain', epoch: 1, address: 'http://plain', status: 'active' });
            try {
                const view = await m2.refresh();
                expect(view.hosts.find((h) => h.hostId === 's.typed')).toEqual(typed);
                // Absent stays absent — never normalized to an empty list.
                expect(view.hosts.find((h) => h.hostId === 's.plain')!.types).toBeUndefined();
                await m1.setStatus('leaving');
                const republished = (await m2.refresh()).hosts.find(
                    (h) => h.hostId === 's.typed'
                )!;
                expect(republished.status).toBe('leaving');
                expect(republished.types).toEqual(['Counter', 'Relay']);
            } finally {
                await m1.leave();
                await m2.leave();
            }
        });

        it('join publishes a live descriptor; leave removes it; views converge', async () => {
            const opts = { schema, heartbeatMs: 100, ttlMs: 400, pollMs: 100 };
            const m1 = pgMembership(pool, opts);
            const m2 = pgMembership(pool, opts);
            await m1.join({ hostId: 's.one', epoch: 1, address: 'http://one', status: 'active' });
            await m2.join({ hostId: 's.two', epoch: 1, address: 'http://two', status: 'active' });
            try {
                await expect(m1.isAlive('s.two')).resolves.toBe(true);
                const view = await m1.refresh();
                expect(view.hosts.map((h) => h.hostId).sort()).toEqual(['s.one', 's.two']);

                await m2.leave();
                await expect(m1.isAlive('s.two')).resolves.toBe(false);
                const after = await m1.refresh();
                expect(after.hosts.map((h) => h.hostId)).toEqual(['s.one']);
            } finally {
                await m2.leave();
                await m1.leave();
            }
        });

        it('a beat that lands past the TTL fires onSelfSuspect even though the write SUCCEEDS (#45)', async () => {
            // Self-suspicion used to fire only from a write REJECTION, so a
            // host whose event loop stalled past the TTL resumed, upserted
            // late, succeeded — and, because the upsert re-creates the row,
            // looked perfectly healthy again while a survivor already held
            // its actors. `heartbeatMs > ttlMs` makes every beat late.
            const m1 = pgMembership(pool, { schema, heartbeatMs: 300, ttlMs: 100, pollMs: 60_000 });
            let suspects = 0;
            m1.onSelfSuspect(() => suspects++);
            await m1.join({ hostId: 's.late', epoch: 1, address: 'http://l', status: 'active' });
            try {
                await vi.waitFor(() => expect(suspects).toBeGreaterThan(0), { timeout: 3000 });
                // Nothing FAILED — the upserts keep landing. Polled, not
                // sampled once: this config leaves the row expired for
                // 200 of every 300 ms by construction, so a bare `isAlive`
                // is a coin flip on where in the beat the assertion lands.
                await vi.waitFor(
                    async () => {
                        await expect(m1.isAlive('s.late')).resolves.toBe(true);
                    },
                    { timeout: 3000 }
                );
            } finally {
                // In a `finally` so a failed assertion cannot leave the beat
                // running — it would keep querying the pool and hang the
                // suite's teardown long after the real failure.
                await m1.leave();
            }
        });

        it('setStatus propagates to peers', async () => {
            const opts = { schema, heartbeatMs: 100, ttlMs: 400, pollMs: 100 };
            const m1 = pgMembership(pool, opts);
            const m2 = pgMembership(pool, opts);
            await m1.join({ hostId: 's.s1', epoch: 1, address: 'http://s1', status: 'active' });
            await m2.join({ hostId: 's.s2', epoch: 1, address: 'http://s2', status: 'active' });
            try {
                await m2.setStatus('leaving');
                const view = await m1.refresh();
                expect(view.hosts.find((h) => h.hostId === 's.s2')?.status).toBe('leaving');
            } finally {
                await m2.leave();
                await m1.leave();
            }
        });

        it('a silent death expires on the DATABASE clock — and the view version moves with it (#267)', async () => {
            const m1 = pgMembership(pool, { schema, heartbeatMs: 100, ttlMs: 400, pollMs: 100 });
            await m1.join({ hostId: 's.w1', epoch: 1, address: 'http://w1', status: 'active' });
            try {
                // A host that died without leaving: a row whose heartbeat
                // never renews. Inserted directly, because the point is
                // exactly that NOTHING announces the death — only expiry
                // reveals it.
                await pool.query(
                    `INSERT INTO ${schema}.hosts (host_id, descriptor, expires_at)
                     VALUES ($1, $2, now() + interval '300 milliseconds')`,
                    [
                        's.ghost',
                        JSON.stringify({
                            hostId: 's.ghost',
                            epoch: 1,
                            address: 'http://ghost',
                            status: 'active'
                        })
                    ]
                );
                await expect(m1.isAlive('s.ghost')).resolves.toBe(true);
                const before = await m1.refresh();
                expect(before.hosts.map((h) => h.hostId).sort()).toEqual(['s.ghost', 's.w1']);
                const counter = async (): Promise<number> =>
                    Number(
                        (
                            await pool.query(
                                `SELECT version FROM ${schema}.membership_version WHERE id = 1`
                            )
                        ).rows[0]!['version']
                    );
                const counterBefore = await counter();

                await vi.waitFor(
                    async () => {
                        await expect(m1.isAlive('s.ghost')).resolves.toBe(false);
                    },
                    { timeout: 2000 }
                );
                // Nothing ever announced this death, so the store's counter
                // does not move — the change detector compares host
                // signatures, not just the counter, and the poll drops the
                // corpse. The VIEW's version must move regardless: it is
                // advanced locally on a signature-only change, so a consumer
                // keyed on it sees the expiry instead of latching the stale
                // member count (#267).
                await vi.waitFor(
                    () => {
                        expect(m1.view().hosts.some((h) => h.hostId === 's.ghost')).toBe(false);
                    },
                    { timeout: 2000 }
                );
                expect(m1.view().hosts.map((h) => h.hostId)).toEqual(['s.w1']);
                expect(m1.view().version).toBeGreaterThan(before.version);
                await expect(counter()).resolves.toBe(counterBefore);
            } finally {
                await m1.leave();
            }
        });

        it('views converge via LISTEN/NOTIFY without waiting out the poll interval', async () => {
            // Polls effectively disabled: convergence must come from push.
            const opts = { schema, heartbeatMs: 60_000, ttlMs: 120_000, pollMs: 60_000 };
            const m1 = pgMembership(pool, opts);
            const m2 = pgMembership(pool, opts);
            await m1.join({ hostId: 's.p1', epoch: 1, address: 'http://p1', status: 'active' });

            const started = Date.now();
            await m2.join({ hostId: 's.p2', epoch: 1, address: 'http://p2', status: 'active' });
            // In a `finally`: this case failed once on CI with a stale host
            // from the case before it (#209 — the heartbeat/leave race), and
            // the un-run `leave()`s then held both LISTEN connections
            // checked out, so `afterAll`'s `pool.end()` hung too.
            try {
                await vi.waitFor(
                    () => {
                        expect(m1.view().hosts.map((h) => h.hostId).sort()).toEqual([
                            's.p1',
                            's.p2'
                        ]);
                    },
                    { timeout: 2000 }
                );
                expect(Date.now() - started).toBeLessThan(2000); // ≪ pollMs
                await m2.leave();
                await vi.waitFor(
                    () => {
                        expect(m1.view().hosts.map((h) => h.hostId)).toEqual(['s.p1']);
                    },
                    { timeout: 2000 }
                );
            } finally {
                await m2.leave();
                await m1.leave();
            }
        });
    });

    describe('2-host smoke over real Postgres', () => {
        const counter = defineActor({
            type: 'Counter',
            allowAnonymous: true,
            state: () => ({ count: 0 }),
            methods: (ctx) => ({
                async increment(by: number) {
                    ctx.state.count += by;
                    await ctx.save();
                    return ctx.state.count;
                }
            })
        });

        it('two hosts share one actor system: single activation, forwarded calls', async () => {
            const secret = 'smoke-secret';
            const registry = new Map<string, { host: Host; placement: ClusterPlacement }>();
            const pipeFetch: typeof globalThis.fetch = async (input, init) => {
                const request = new Request(input, init);
                const member = registry.get(new URL(request.url).host);
                if (!member) throw new TypeError('connection refused');
                return matchesHostRequest(request)
                    ? handleHostRequest(request, { ...member, secret })
                    : handleActorRequest(request, { host: member.host, origin: false });
            };
            // Storage, membership and directory all on the ONE pool — the
            // deployment shape this package exists for.
            const storage = pgStorage({ pool, schema });
            const hosts: Host[] = [];
            for (let i = 0; i < 2; i++) {
                const placement = clusterPlacement({
                    ...pgCluster({ pool, schema }),
                    advertise: `http://smoke${i}.test`,
                    secret,
                    fetch: pipeFetch
                });
                const host = createHost({
                    actors: [counter],
                    storage,
                    placement,
                    defaults: { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 }
                });
                registry.set(`smoke${i}.test`, { host, placement });
                hosts.push(host);
                await host.start();
            }
            try {
                const [a, b] = await Promise.all([
                    hosts[0]!.actor(counter, 'shared').increment(1),
                    hosts[1]!.actor(counter, 'shared').increment(1)
                ]);
                expect([a, b].sort()).toEqual([1, 2]);
                await expect(hosts[0]!.actor(counter, 'shared').increment(1)).resolves.toBe(3);
                await expect(hosts[1]!.actor(counter, 'shared').increment(1)).resolves.toBe(4);
                // One activation: exactly one directory row for THIS actor
                // id (other tests in the shared schema also claim Counter-
                // prefixed ids, so the match must be exact and escaped).
                const rows = await pool.query(
                    `SELECT host_id FROM ${schema}.directory WHERE actor_id = $1`,
                    [pgText(`Counter${String.fromCharCode(0)}shared`)]
                );
                expect(rows.rowCount).toBe(1);
            } finally {
                await Promise.allSettled(hosts.map((h) => h.stop({ timeoutMs: 1000 })));
            }
        });
    });
});

describe.runIf(!PG_URL)('pg cluster providers (no PG_URL)', () => {
    it('skips the Postgres suite when PG_URL is not set', () => {
        expect(PG_URL).toBeUndefined();
    });
});
