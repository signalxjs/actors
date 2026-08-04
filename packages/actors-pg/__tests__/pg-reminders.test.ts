/**
 * pgReminders tests — env-gated on `PG_URL`. Covers the ReminderApi
 * contract (set/list/clear with NUL-bearing refs, the 60s period floor),
 * the claim semantics (one-shot fires once and disappears; periodic
 * advances before delivery; no catch-up bursts), the SKIP LOCKED
 * at-most-once guarantee across two concurrently-ticking providers, and an
 * end-to-end `createHost` wiring where a real actor's `onReminder` fires.
 */
import { afterAll, beforeAll, describe, expect, it, vi } from 'vitest';
import pg from 'pg';
import { defineActor, type ActorRemindersContext, type ActorRef } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';
import { ensurePgSchema, pgReminders, pgStorage } from '@sigx/actors-pg';

const PG_URL = process.env.PG_URL;
const NUL = String.fromCharCode(0);

describe.skipIf(!PG_URL)('pgReminders', () => {
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

    /** A provider bound to a manual clock and a recording deliver. */
    function bound(tickMs = 1_000) {
        const scheduler = manualScheduler();
        const delivered: { ref: ActorRef; name: string }[] = [];
        const provider = pgReminders({ pool, schema });
        const context: ActorRemindersContext = {
            storage: memoryStorage(),
            scheduler,
            tickMs,
            ownsShard: () => true,
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
        await expect(provider.apiFor({ type: 'Room', key: 'general' }).list()).resolves.toEqual(
            []
        );
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
        // Still registered — and already advanced ~60s on the DB clock, so
        // an immediate second tick finds nothing due (no catch-up burst).
        await expect(provider.apiFor(ref).list()).resolves.toEqual(['pulse']);
        scheduler.advance(1_000);
        await new Promise((r) => setTimeout(r, 150));
        expect(delivered).toHaveLength(1);
        await provider.apiFor(ref).clear('pulse');
        await provider.stop();
    });

    it('two ticking providers deliver a due reminder exactly once (SKIP LOCKED)', async () => {
        const a = bound();
        const b = bound();
        const ref = { type: 'Once', key: 'contended' };
        await a.provider.apiFor(ref).set('only', { due: 0 });
        a.provider.start();
        b.provider.start();
        // Both hosts tick concurrently against the same table.
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
            storage: pgStorage({ pool, schema }),
            reminders: pgReminders({ pool, schema }),
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

describe.runIf(!PG_URL)('pgReminders (no PG_URL)', () => {
    it('skips the Postgres suite when PG_URL is not set', () => {
        expect(PG_URL).toBeUndefined();
    });
});
