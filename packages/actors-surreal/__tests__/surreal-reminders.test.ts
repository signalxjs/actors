/**
 * surrealReminders tests — env-gated on `SURREAL_URL`. Covers the
 * ReminderApi contract (set/list/clear with NUL-bearing refs, the 60s period
 * floor), the claim semantics (one-shot fires once and disappears; periodic
 * advances before delivery; no catch-up bursts), the shard-ownership
 * partitioning that stands in for `SKIP LOCKED`, at-most-once across two
 * concurrently-ticking providers, the retry of a dispatch that failed (#326),
 * and an end-to-end `createHost` wiring where a real actor's `onReminder`
 * fires.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import { RecordId } from 'surrealdb';
import { defineActor, type ActorRef, type ActorRemindersContext } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';
import { ensureSurrealSchema, surrealReminders, surrealStorage } from '@sigx/actors-surreal';
import { connect, dropNamespace, testNamespace, type TestDb } from './helpers';

const SURREAL_URL = process.env.SURREAL_URL;
const NUL = String.fromCharCode(0);

describe.skipIf(!SURREAL_URL)('surrealReminders', () => {
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

    /**
     * A provider bound to a manual clock and a recording deliver. `fail`
     * decides per attempt whether the dispatch rejects; every attempt is
     * recorded in `delivered`, every failure in `undelivered`.
     */
    function bound(
        tickMs = 1_000,
        ownsShard: (shard: string) => boolean = () => true,
        fail: (ref: ActorRef, name: string) => boolean | Promise<boolean> = () => false
    ) {
        const scheduler = manualScheduler();
        const delivered: { ref: ActorRef; name: string }[] = [];
        const undelivered: { ref: ActorRef; name: string; error: unknown }[] = [];
        const provider = surrealReminders({ db });
        const context: ActorRemindersContext = {
            storage: memoryStorage(),
            scheduler,
            tickMs,
            ownsShard,
            deliver: async (ref, name) => {
                delivered.push({ ref, name });
                if (await fail(ref, name)) throw new Error('dispatch deadline');
            },
            undelivered: (ref, name, error) => void undelivered.push({ ref, name, error })
        };
        provider.bind(context);
        return { provider, scheduler, delivered, undelivered };
    }

    /** The row as stored — `dueInMs` is `d - time::now()` on the DB clock. */
    async function rowOf(ref: ActorRef, name: string) {
        const [row] = await db.query<[{ p: number; d: number; now: number } | null]>(
            `SELECT p, time::millis(d) AS d, time::millis(time::now()) AS now FROM ONLY $id`,
            { id: new RecordId('sigx_reminder', [ref.type, ref.key, name]) }
        );
        return row ? { dueInMs: row.d - row.now, period: row.p === 0 ? undefined : row.p } : undefined;
    }

    it('set/list/clear round-trip, with NUL and backslashes in the ref and name', async () => {
        const { provider } = bound();
        const ref = { type: 'Room', key: `general${NUL}with\\slash` };
        const api = provider.apiFor(ref);
        await api.set('cleanup', { due: 3_600_000 });
        await api.set(`odd${NUL}name`, { due: 3_600_000, period: 60_000 });
        await expect(api.list()).resolves.toEqual(['cleanup', `odd${NUL}name`]);
        // A different key is a different reminder set.
        await expect(provider.apiFor({ type: 'Room', key: 'general' }).list()).resolves.toEqual([]);
        await api.clear('cleanup');
        await api.clear(`odd${NUL}name`);
        await expect(api.list()).resolves.toEqual([]);
    });

    it('rejects periods under the 60s floor', async () => {
        const { provider } = bound();
        const api = provider.apiFor({ type: 'T', key: 'k' });
        await expect(api.set('fast', { due: 0, period: 1_000 })).rejects.toThrow(/floor/);
    });

    it('a one-shot fires once and its row is gone before delivery', async () => {
        const { provider, scheduler, delivered } = bound();
        const ref = { type: 'Shot', key: `k${NUL}1` };
        await provider.apiFor(ref).set('once', { due: 0 });
        provider.start();
        scheduler.advance(1_000);
        await vi.waitFor(() => expect(delivered).toHaveLength(1));
        expect(delivered[0]).toEqual({ ref, name: 'once' });
        await expect(provider.apiFor(ref).list()).resolves.toEqual([]);
        // Further ticks deliver nothing.
        scheduler.advance(2_000);
        await new Promise((r) => setTimeout(r, 150));
        expect(delivered).toHaveLength(1);
        await provider.stop();
    });

    it('a periodic reminder advances BEFORE delivery and does not burst', async () => {
        const { provider, scheduler, delivered } = bound();
        const ref = { type: 'Beat', key: 'k' };
        await provider.apiFor(ref).set('pulse', { due: 0, period: 60_000 });
        provider.start();
        scheduler.advance(1_000);
        await vi.waitFor(() => expect(delivered).toHaveLength(1));
        // Still registered — and already advanced ~60s on the DB clock, so an
        // immediate second tick finds nothing due (no catch-up burst).
        await expect(provider.apiFor(ref).list()).resolves.toEqual(['pulse']);
        scheduler.advance(1_000);
        await new Promise((r) => setTimeout(r, 150));
        expect(delivered).toHaveLength(1);
        await provider.apiFor(ref).clear('pulse');
        await provider.stop();
    });

    it('a host that owns no shard delivers nothing', async () => {
        // The partitioning that replaces SKIP LOCKED: ownership, not locking,
        // is what keeps two hosts off each other's rows.
        const idle = bound(1_000, () => false);
        const ref = { type: 'Idle', key: 'k' };
        await idle.provider.apiFor(ref).set('never', { due: 0 });
        idle.provider.start();
        idle.scheduler.advance(1_000);
        await new Promise((r) => setTimeout(r, 250));
        expect(idle.delivered).toHaveLength(0);
        // Still due — nobody claimed it.
        await expect(idle.provider.apiFor(ref).list()).resolves.toEqual(['never']);
        await idle.provider.stop();
        await idle.provider.apiFor(ref).clear('never');
    });

    it('disjoint shard owners split the work with no overlap', async () => {
        // The steady state: each host claims only its own shards, so the two
        // never contend at all.
        const even = bound(1_000, (s) => Number(s.slice(1)) % 2 === 0);
        const odd = bound(1_000, (s) => Number(s.slice(1)) % 2 === 1);
        const refs = Array.from({ length: 12 }, (_v, i) => ({ type: 'Split', key: `k${i}` }));
        for (const ref of refs) await even.provider.apiFor(ref).set('go', { due: 0 });
        even.provider.start();
        odd.provider.start();
        even.scheduler.advance(1_000);
        odd.scheduler.advance(1_000);
        await vi.waitFor(() =>
            expect(even.delivered.length + odd.delivered.length).toBe(refs.length)
        );
        // Both did some of it, and no reminder went to both.
        expect(even.delivered.length).toBeGreaterThan(0);
        expect(odd.delivered.length).toBeGreaterThan(0);
        const keys = [...even.delivered, ...odd.delivered].map((d) => d.ref.key);
        expect(new Set(keys).size).toBe(refs.length);
        await even.provider.stop();
        await odd.provider.stop();
    });

    it('two providers on the SAME shards deliver exactly once', async () => {
        // Transient ownership divergence: both believe they own everything.
        // The claim is one transaction, so the loser aborts at commit and
        // re-selects an already-advanced set — the conflict IS the exclusion.
        const a = bound();
        const b = bound();
        const ref = { type: 'Once', key: 'contended' };
        await a.provider.apiFor(ref).set('only', { due: 0 });
        a.provider.start();
        b.provider.start();
        a.scheduler.advance(1_000);
        b.scheduler.advance(1_000);
        await vi.waitFor(() =>
            expect(a.delivered.length + b.delivered.length).toBeGreaterThanOrEqual(1)
        );
        await new Promise((r) => setTimeout(r, 250));
        expect(a.delivered.length + b.delivered.length).toBe(1);
        await a.provider.stop();
        await b.provider.stop();
    });

    describe('a dispatch that fails (#326)', () => {
        // The claim advances or deletes the row BEFORE `deliver()`, so a
        // rejected dispatch — a deadline, an `onReminder` that threw — used
        // to be the end of the wake. It is now re-armed one tick out on the
        // DB clock and reported through `context.undelivered`, with the same
        // rules as `shardedReminders()` (#306).
        const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
        const TICK = 200;
        const failing = (fail: (ref: ActorRef, name: string) => boolean | Promise<boolean>) =>
            bound(TICK, () => true, fail);

        it('re-arms a one-shot whose dispatch failed one tick out, and reports it', async () => {
            let fail = true;
            const { provider, scheduler, delivered, undelivered } = failing(() => fail);
            const ref = { type: 'Retry', key: `shot${NUL}1` };
            const api = provider.apiFor(ref);
            await api.set('wake', { due: 0 });
            provider.start();
            try {
                scheduler.advance(TICK); // tick 1: deliver rejects
                await vi.waitFor(() => expect(delivered).toHaveLength(1));
                expect(undelivered).toHaveLength(1);
                expect(undelivered[0]).toMatchObject({ ref, name: 'wake' });
                expect((undelivered[0]!.error as Error).message).toBe('dispatch deadline');
                // Still registered — one tick out, not dropped.
                await vi.waitFor(async () => {
                    const row = await rowOf(ref, 'wake');
                    expect(row).toBeDefined();
                    expect(row!.period).toBeUndefined();
                    expect(row!.dueInMs).toBeGreaterThan(0);
                    expect(row!.dueInMs).toBeLessThanOrEqual(TICK);
                });
                // Not sooner than that: an immediate tick finds nothing due.
                scheduler.advance(TICK);
                await sleep(100);
                expect(delivered).toHaveLength(1);

                fail = false;
                await sleep(TICK);
                scheduler.advance(TICK); // tick 3: delivered
                await vi.waitFor(() => expect(delivered).toHaveLength(2));
                expect(delivered[1]).toEqual({ ref, name: 'wake' });
                // A one-shot that finally fired clears itself.
                await expect(api.list()).resolves.toEqual([]);
                expect(undelivered).toHaveLength(1);
            } finally {
                await provider.stop();
            }
        });

        it('retries a periodic reminder one tick out, then resumes its cadence', async () => {
            let fail = true;
            const { provider, scheduler, delivered, undelivered } = failing(() => fail);
            const ref = { type: 'Retry', key: 'beat' };
            const api = provider.apiFor(ref);
            await api.set('beat', { due: 0, period: 60_000 });
            provider.start();
            try {
                scheduler.advance(TICK);
                await vi.waitFor(() => expect(delivered).toHaveLength(1));
                expect(undelivered).toHaveLength(1);
                // Pulled forward to the next tick, not left a period out.
                await vi.waitFor(async () => {
                    const row = await rowOf(ref, 'beat');
                    expect(row!.period).toBe(60_000);
                    expect(row!.dueInMs).toBeLessThanOrEqual(TICK);
                });

                fail = false;
                await sleep(TICK);
                scheduler.advance(TICK);
                await vi.waitFor(() => expect(delivered).toHaveLength(2));
                // A SUCCESSFUL firing advances by the period, as before.
                const row = await rowOf(ref, 'beat');
                expect(row!.dueInMs).toBeGreaterThan(60_000 - TICK * 2);
                expect(undelivered).toHaveLength(1);
            } finally {
                await provider.stop();
                await api.clear('beat');
            }
        });

        it('a permanently failing target costs one attempt per tick, each reported', async () => {
            const { provider, scheduler, delivered, undelivered } = failing(() => true);
            const ref = { type: 'Retry', key: 'never' };
            const api = provider.apiFor(ref);
            await api.set('wake', { due: 0 });
            provider.start();
            try {
                for (let tick = 1; tick <= 3; tick++) {
                    scheduler.advance(TICK);
                    await vi.waitFor(() => expect(delivered).toHaveLength(tick));
                    // Never a hot loop: nothing more until the row is due
                    // again AND the clock ticks.
                    await sleep(TICK + 50);
                    expect(delivered).toHaveLength(tick);
                }
                expect(undelivered).toHaveLength(3);
                await expect(api.list()).resolves.toEqual(['wake']);
            } finally {
                await provider.stop();
                await api.clear('wake');
            }
        });

        it('leaves a reminder the actor set or cleared during the failing dispatch alone', async () => {
            // A later decision wins: the re-arm only touches a row that is
            // still exactly as the claim left it.
            const ref = { type: 'Retry', key: 'meanwhile' };
            const { provider, scheduler, delivered } = failing(async (_ref, name) => {
                // `onReminder` rescheduling itself, then timing out.
                if (name === 'wake') await provider.apiFor(ref).set('wake', { due: 3_600_000 });
                if (name === 'beat') await provider.apiFor(ref).clear('beat');
                return true;
            });
            const api = provider.apiFor(ref);
            await api.set('wake', { due: 0 });
            await api.set('beat', { due: 0, period: 60_000 });
            provider.start();
            try {
                scheduler.advance(TICK);
                await vi.waitFor(() => expect(delivered).toHaveLength(2));
                await sleep(100); // let the re-arm land
                // The one-shot is as the actor re-set it, not one tick out…
                const wake = await rowOf(ref, 'wake');
                expect(wake!.dueInMs).toBeGreaterThan(3_600_000 - TICK * 2);
                // …and the cleared periodic stays cleared.
                await expect(rowOf(ref, 'beat')).resolves.toBeUndefined();
            } finally {
                await provider.stop();
                await api.clear('wake');
            }
        });
    });

    it('bind() twice is refused — one provider per host', () => {
        const { provider } = bound();
        expect(() =>
            provider.bind({
                storage: memoryStorage(),
                scheduler: manualScheduler(),
                tickMs: 1_000,
                ownsShard: () => true,
                deliver: () => Promise.resolve()
            })
        ).toThrow(/already bound/);
    });

    it('end to end: a real actor’s onReminder fires through createHost wiring', async () => {
        const fired: string[] = [];
        const Pinger = defineActor({
            type: 'Pinger',
            allowAnonymous: true,
            state: () => ({ armed: false }),
            methods: (ctx) => ({
                async arm() {
                    await ctx.reminders.set('ping', { due: 0 });
                    ctx.state.armed = true;
                }
            }),
            onReminder: (_ctx, name) => {
                fired.push(name);
            }
        });
        const scheduler = manualScheduler();
        const host = createHost({
            actors: [Pinger],
            storage: surrealStorage({ db }),
            reminders: surrealReminders({ db }),
            scheduler,
            defaults: { sweepIntervalMs: 0, reminderTickMs: 1_000, callTimeoutMs: 0 }
        });
        await host.start();
        try {
            await host.actor(Pinger, 'p1').arm();
            scheduler.advance(1_000);
            await vi.waitFor(() => expect(fired).toEqual(['ping']));
        } finally {
            await host.stop({ timeoutMs: 1000 });
        }
    });
});

describe.runIf(!SURREAL_URL)('surrealReminders (no SURREAL_URL)', () => {
    it('skips the SurrealDB suite when SURREAL_URL is not set', () => {
        expect(SURREAL_URL).toBeUndefined();
    });
});
