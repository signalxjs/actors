/**
 * SurrealDB provider tests — env-gated on `SURREAL_URL`. Covers the provider
 * contracts (claim CAS matrix, compare-and-delete, membership heartbeat +
 * database-clock expiry, live-query push) and a 2-host end-to-end smoke over
 * a real SurrealDB — storage, membership and directory all riding one
 * connection.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { RecordId } from 'surrealdb';
import { defineActor, type Host } from '@sigx/actors';
import { createHost } from '@sigx/actors/host';
import { handleActorRequest } from '@sigx/actors/server';
import {
    clusterPlacement,
    handleHostRequest,
    matchesHostRequest,
    type ClusterPlacement
} from '@sigx/actors/cluster';
import {
    ensureSurrealSchema,
    surrealCluster,
    surrealDirectory,
    surrealMembership,
    surrealStorage
} from '@sigx/actors-surreal';
import { connect, dropNamespace, testNamespace, type TestDb } from './helpers';

const SURREAL_URL = process.env.SURREAL_URL;
const NUL = String.fromCharCode(0);

describe.skipIf(!SURREAL_URL)('surreal cluster providers', () => {
    let db: TestDb;
    let namespace: string;

    beforeAll(async () => {
        namespace = testNamespace();
        db = await connect(namespace);
        await ensureSurrealSchema(db);
    });
    afterAll(async () => {
        await dropNamespace(db, namespace);
    });

    describe('options', () => {
        it('refuses an unsafe table prefix everywhere it is taken', () => {
            expect(() => surrealDirectory({ db, prefix: 'bad-name' })).toThrow(/identifier/);
            expect(() => surrealMembership({ db, prefix: 'x; REMOVE TABLE t' })).toThrow(
                /identifier/
            );
            expect(() => surrealStorage({ db, prefix: 'Sigx' })).toThrow(/identifier/);
        });

        it('requires either a connection or a url', () => {
            expect(() => surrealStorage({})).toThrow(/either `db`.*or `url`/);
        });
    });

    describe('directory', () => {
        it('claim is create-if-absent and returns the winner', async () => {
            const dir = surrealDirectory({ db });
            const a = { hostId: 's.a', activationId: 's.a/1/1' };
            const b = { hostId: 's.b', activationId: 's.b/1/1' };
            const id = `Widget${NUL}w1`;
            await expect(dir.claim(id, a)).resolves.toEqual(a);
            // The loser is told who won, never that it won.
            await expect(dir.claim(id, b)).resolves.toEqual(a);
            await expect(dir.lookup(id)).resolves.toEqual(a);
            await dir.release(id, a);
            await expect(dir.lookup(id)).resolves.toBeNull();
        });

        it('carries a real `type<NUL>key` actor id', async () => {
            const dir = surrealDirectory({ db });
            const mine = { hostId: 's.n', activationId: 's.n/1/1' };
            await expect(dir.claim(`Counter${NUL}shared`, mine)).resolves.toEqual(mine);
            // Injective: a different split of the same characters is a
            // different actor.
            await expect(dir.lookup(`Counter${NUL}share`)).resolves.toBeNull();
            await expect(dir.lookup(`Counte${NUL}rshared`)).resolves.toBeNull();
            await dir.release(`Counter${NUL}shared`, mine);
        });

        it('release and evict are compare-and-delete', async () => {
            const dir = surrealDirectory({ db });
            const mine = { hostId: 's.c', activationId: 's.c/1/1' };
            const other = { hostId: 's.c', activationId: 's.c/1/2' };
            const id = `Widget${NUL}w2`;
            await dir.claim(id, mine);
            // A stale activation id must not delete the live claim.
            await dir.release(id, other);
            await expect(dir.lookup(id)).resolves.toEqual(mine);
            await expect(dir.evict(id, other)).resolves.toBe(false);
            await expect(dir.evict(id, mine)).resolves.toBe(true);
            await expect(dir.lookup(id)).resolves.toBeNull();
            await expect(dir.evict(id, mine)).resolves.toBe(false);
        });

        it('evictHost sweeps exactly one host', async () => {
            const dir = surrealDirectory({ db });
            await dir.claim(`Ev${NUL}1`, { hostId: 's.gone', activationId: 's.gone/1/1' });
            await dir.claim(`Ev${NUL}2`, { hostId: 's.gone', activationId: 's.gone/1/2' });
            await dir.claim(`Ev${NUL}3`, { hostId: 's.stay', activationId: 's.stay/1/1' });
            await expect(dir.evictHost!('s.gone')).resolves.toBe(2);
            await expect(dir.lookup(`Ev${NUL}1`)).resolves.toBeNull();
            await expect(dir.lookup(`Ev${NUL}3`)).resolves.toEqual({
                hostId: 's.stay',
                activationId: 's.stay/1/1'
            });
            await expect(dir.evictHost!('s.nobody')).resolves.toBe(0);
        });

        it('two racing claims agree on one winner', async () => {
            // No lock primitive exists: correctness comes from the
            // commit-time write–write conflict plus the connection's retry.
            const dir = surrealDirectory({ db });
            const a = { hostId: 's.r1', activationId: 's.r1/1/1' };
            const b = { hostId: 's.r2', activationId: 's.r2/1/1' };
            const id = `Race${NUL}1`;
            const [first, second] = await Promise.all([dir.claim(id, a), dir.claim(id, b)]);
            expect(first).toEqual(second);
            expect([a, b]).toContainEqual(first);
            await expect(dir.lookup(id)).resolves.toEqual(first);
        });
    });

    describe('membership', () => {
        const opts = { heartbeatMs: 100, ttlMs: 400, pollMs: 100 };

        it('HostDescriptor.types round-trips join → view and survives setStatus (#214)', async () => {
            // Deep-equal on the WHOLE descriptor: a provider that projects
            // known fields instead of round-tripping would drop `types` and
            // silently make a heterogeneous cluster eligible-for-everything.
            const m1 = surrealMembership({ db, ...opts });
            const m2 = surrealMembership({ db, ...opts });
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
            const m1 = surrealMembership({ db, ...opts });
            const m2 = surrealMembership({ db, ...opts });
            await m1.join({ hostId: 's.one', epoch: 1, address: 'http://one', status: 'active' });
            await m2.join({ hostId: 's.two', epoch: 1, address: 'http://two', status: 'active' });
            await expect(m1.isAlive('s.two')).resolves.toBe(true);
            const view = await m1.refresh();
            expect(view.hosts.map((h) => h.hostId).sort()).toEqual(['s.one', 's.two']);

            await m2.leave();
            await expect(m1.isAlive('s.two')).resolves.toBe(false);
            const after = await m1.refresh();
            expect(after.hosts.map((h) => h.hostId)).toEqual(['s.one']);
            await m1.leave();
        });

        it('a beat that lands past the TTL fires onSelfSuspect even though the write SUCCEEDS (#45)', async () => {
            // Self-suspicion used to fire only from a write REJECTION, so a
            // host whose event loop stalled past the TTL resumed, upserted
            // late, succeeded — and, because the UPSERT re-creates the
            // record, looked healthy again while a survivor already held its
            // actors. `heartbeatMs > ttlMs` makes every beat late.
            const m1 = surrealMembership({ db, heartbeatMs: 300, ttlMs: 100, pollMs: 60_000 });
            let suspects = 0;
            m1.onSelfSuspect(() => suspects++);
            await m1.join({ hostId: 's.late', epoch: 1, address: 'http://l', status: 'active' });
            try {
                await vi.waitFor(() => expect(suspects).toBeGreaterThan(0), { timeout: 3000 });
                // Nothing FAILED — the upserts keep landing. Polled, not
                // sampled once: this config leaves the record expired for
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
                // running against a connection the suite is about to close.
                await m1.leave();
            }
        });

        it('setStatus propagates to peers', async () => {
            const m1 = surrealMembership({ db, ...opts });
            const m2 = surrealMembership({ db, ...opts });
            await m1.join({ hostId: 's.s1', epoch: 1, address: 'http://s1', status: 'active' });
            await m2.join({ hostId: 's.s2', epoch: 1, address: 'http://s2', status: 'active' });
            await m2.setStatus('leaving');
            const view = await m1.refresh();
            expect(view.hosts.find((h) => h.hostId === 's.s2')?.status).toBe('leaving');
            await m2.leave();
            await m1.leave();
        });

        it('a silent death expires on the DATABASE clock — no version bump needed', async () => {
            const m1 = surrealMembership({ db, ...opts });
            await m1.join({ hostId: 's.w1', epoch: 1, address: 'http://w1', status: 'active' });
            // A host that died without leaving: a record whose heartbeat never
            // renews. Written directly, because the point is exactly that
            // NOTHING announces the death — only expiry reveals it. The expiry
            // is computed by the SERVER, so a skewed client cannot fake it.
            await db.query(
                'UPSERT $id CONTENT { d: $d, x: time::now() + duration::from_millis(300) } RETURN NONE',
                {
                    id: new RecordId('sigx_host', 's.ghost'),
                    d: JSON.stringify({
                        hostId: 's.ghost',
                        epoch: 1,
                        address: 'http://ghost',
                        status: 'active'
                    })
                }
            );
            await expect(m1.isAlive('s.ghost')).resolves.toBe(true);
            const before = await m1.refresh();
            expect(before.hosts.map((h) => h.hostId).sort()).toEqual(['s.ghost', 's.w1']);

            await vi.waitFor(
                async () => {
                    await expect(m1.isAlive('s.ghost')).resolves.toBe(false);
                },
                { timeout: 3000 }
            );
            // The cached view updates WITHOUT any version bump: the change
            // detector compares host signatures, not just the counter —
            // nothing ever announced this death, yet the poll drops it.
            await vi.waitFor(
                () => {
                    expect(m1.view().hosts.some((h) => h.hostId === 's.ghost')).toBe(false);
                },
                { timeout: 3000 }
            );
            expect(m1.view().hosts.map((h) => h.hostId)).toEqual(['s.w1']);
            await m1.leave();
        });

        it('views converge via the live query without waiting out the poll interval', async () => {
            // Polls effectively disabled: convergence must come from push.
            const pushed = { heartbeatMs: 60_000, ttlMs: 120_000, pollMs: 60_000 };
            const m1 = surrealMembership({ db, ...pushed });
            const m2 = surrealMembership({ db, ...pushed });
            await m1.join({ hostId: 's.p1', epoch: 1, address: 'http://p1', status: 'active' });

            const started = Date.now();
            await m2.join({ hostId: 's.p2', epoch: 1, address: 'http://p2', status: 'active' });
            await vi.waitFor(
                () => {
                    expect(m1.view().hosts.map((h) => h.hostId).sort()).toEqual(['s.p1', 's.p2']);
                },
                { timeout: 5000 }
            );
            expect(Date.now() - started).toBeLessThan(5000); // ≪ pollMs
            await m2.leave();
            await vi.waitFor(
                () => {
                    expect(m1.view().hosts.map((h) => h.hostId)).toEqual(['s.p1']);
                },
                { timeout: 5000 }
            );
            await m1.leave();
        });

        it('push: false still converges on the poll', async () => {
            const polled = { heartbeatMs: 100, ttlMs: 400, pollMs: 100, push: false };
            const m1 = surrealMembership({ db, ...polled });
            const m2 = surrealMembership({ db, ...polled });
            await m1.join({ hostId: 's.q1', epoch: 1, address: 'http://q1', status: 'active' });
            await m2.join({ hostId: 's.q2', epoch: 1, address: 'http://q2', status: 'active' });
            await vi.waitFor(
                () => {
                    expect(m1.view().hosts.map((h) => h.hostId).sort()).toEqual(['s.q1', 's.q2']);
                },
                { timeout: 3000 }
            );
            await m2.leave();
            await m1.leave();
        });
    });

    describe('2-host smoke over real SurrealDB', () => {
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
            // Storage, membership and directory all on the ONE connection —
            // the deployment shape this package exists for.
            const storage = surrealStorage({ db });
            const hosts: Host[] = [];
            for (let i = 0; i < 2; i++) {
                const placement = clusterPlacement({
                    ...surrealCluster({ db }),
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
                // One activation: exactly one directory record for THIS actor
                // id (other tests in the shared namespace also claim
                // Counter-prefixed ids, so the match must be exact).
                const [rows] = await db.query<[unknown[]]>(
                    'SELECT VALUE h FROM sigx_dir WHERE id = $id',
                    { id: new RecordId('sigx_dir', `Counter${NUL}shared`) }
                );
                expect(rows).toHaveLength(1);
            } finally {
                await Promise.allSettled(hosts.map((h) => h.stop({ timeoutMs: 1000 })));
            }
        });
    });
});

describe.runIf(!SURREAL_URL)('surreal cluster providers (no SURREAL_URL)', () => {
    it('skips the SurrealDB suite when SURREAL_URL is not set', () => {
        expect(SURREAL_URL).toBeUndefined();
    });
});
