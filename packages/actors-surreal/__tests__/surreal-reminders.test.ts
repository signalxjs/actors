/**
 * surrealReminders tests — env-gated on `SURREAL_URL`. Covers the
 * ReminderApi contract (set/list/clear with NUL-bearing refs, the 60s period
 * floor), the claim semantics (one-shot fires once and disappears; periodic
 * advances before delivery; no catch-up bursts), the shard-ownership
 * partitioning that stands in for `SKIP LOCKED`, at-most-once across two
 * concurrently-ticking providers, and an end-to-end `createHost` wiring where
 * a real actor's `onReminder` fires.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
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

    /** A provider bound to a manual clock and a recording deliver. */
    function bound(
        tickMs = 1_000,
        ownsShard: (shard: string) => boolean = () => true
    ) {
        const scheduler = manualScheduler();
        const delivered: { ref: ActorRef; name: string }[] = [];
        const provider = surrealReminders({ db });
        const context: ActorRemindersContext = {
            storage: memoryStorage(),
            scheduler,
            tickMs,
            ownsShard,
            deliver: (ref, name) => {
                delivered.push({ ref, name });
                return Promise.resolve();
            }
        };
        provider.bind(context);
        return { provider, scheduler, delivered };
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
            unguarded: true,
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
