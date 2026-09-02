/**
 * #326 — a reminder dispatch that fails is reported and re-armed, not lost.
 *
 * Not env-gated: the claim is answered by a fake pool, which the structural
 * `PgQueryable` seam exists to allow, so this runs on every leg. What the
 * SQL actually does to the rows is proven against a real Postgres in
 * `pg-reminders.test.ts`; this file pins the control flow around it — every
 * failed attempt (a rejection AND a synchronous throw) reaches
 * `context.undelivered`, the batch's failures go back in ONE statement, a
 * clean batch writes nothing, and neither a throwing reporter nor a failing
 * re-arm can kill the tick loop.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import type { ActorRef } from '@sigx/actors';
import { manualScheduler, memoryStorage } from '@sigx/actors/host';
import { pgReminders, type PgQueryable } from '../src';

const isClaim = (text: string): boolean => text.includes('FOR UPDATE SKIP LOCKED');
const isRearm = (text: string): boolean =>
    text.includes('ON CONFLICT (type, key, name) DO NOTHING');

/** Two claimed rows: a periodic one (advanced) and a one-shot (deleted). */
const CLAIMED = [
    { type: 'Beat', key: 'k', name: 'beat', advanced: '2026-09-02 10:00:00.123456+00' },
    { type: 'Shot', key: 'k', name: 'wake', advanced: null }
];

function fakePool(options: { rearmFails?: boolean } = {}) {
    const statements: { text: string; values: unknown[] | undefined }[] = [];
    let claims = 0;
    const pool: PgQueryable = {
        query(text, values) {
            statements.push({ text, values });
            if (isClaim(text)) {
                // Due once; every later tick finds nothing.
                const rows = claims++ === 0 ? CLAIMED : [];
                return Promise.resolve({ rows, rowCount: rows.length });
            }
            if (isRearm(text) && options.rearmFails) {
                return Promise.reject(new Error('connection reset'));
            }
            return Promise.resolve({ rows: [], rowCount: 0 });
        }
    };
    return {
        pool,
        claims: () => statements.filter((s) => isClaim(s.text)),
        rearms: () => statements.filter((s) => isRearm(s.text))
    };
}

function bound(
    pool: PgQueryable,
    deliver: (ref: ActorRef, name: string) => Promise<unknown>,
    undelivered?: (ref: ActorRef, name: string, error: unknown) => void
) {
    const scheduler = manualScheduler();
    const provider = pgReminders({ pool });
    provider.bind({
        storage: memoryStorage(),
        scheduler,
        tickMs: 250,
        ownsShard: () => true,
        deliver,
        ...(undelivered ? { undelivered } : {})
    });
    provider.start();
    return { provider, scheduler };
}

/**
 * Advance the clock until the loop has claimed `n` times. The loop is
 * single-flight, so an advance that lands while the previous tick is still
 * settling is skipped — keep advancing until one is taken. (The claim is
 * issued synchronously inside the tick, so the count is exact.)
 */
const tickAgain = (
    scheduler: ReturnType<typeof manualScheduler>,
    fake: { claims: () => unknown[] },
    n: number
): Promise<void> =>
    vi.waitFor(() => {
        scheduler.advance(250);
        expect(fake.claims()).toHaveLength(n);
    });

afterEach(() => {
    vi.restoreAllMocks();
});

describe('pgReminders: a dispatch that fails (#326)', () => {
    it('reports every failed attempt and re-arms the batch in one statement', async () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        const fake = fakePool();
        const undelivered: { ref: ActorRef; name: string; error: unknown }[] = [];
        const { provider, scheduler } = bound(
            fake.pool,
            (_ref, name) => {
                // One rejection, one synchronous throw — both are failures.
                if (name === 'beat') return Promise.reject(new Error('dispatch deadline'));
                throw new Error('sync throw');
            },
            (ref, name, error) => void undelivered.push({ ref, name, error })
        );
        try {
            scheduler.advance(250);
            await vi.waitFor(() => expect(fake.rearms()).toHaveLength(1));
            expect(undelivered.map((u) => u.name).sort()).toEqual(['beat', 'wake']);
            expect(undelivered.find((u) => u.name === 'beat')).toMatchObject({
                ref: { type: 'Beat', key: 'k' },
                error: new Error('dispatch deadline')
            });
            expect(undelivered.find((u) => u.name === 'wake')).toMatchObject({
                ref: { type: 'Shot', key: 'k' },
                error: new Error('sync throw')
            });
            // One write for the batch: parallel arrays of the failed rows
            // (in settlement order — the synchronous throw lands first), the
            // periodic one carrying the `next_due` the claim wrote (so the
            // re-arm can tell it from one the actor has since re-set), the
            // one-shot `null`, and the tick to re-arm out to.
            const [rearm] = fake.rearms();
            const [types, keys, names, advanced, tick] = rearm!.values as [
                string[],
                string[],
                string[],
                (string | null)[],
                number
            ];
            const rows = names
                .map((name, i) => ({ type: types[i], key: keys[i], name, advanced: advanced[i] }))
                .sort((x, y) => x.name.localeCompare(y.name));
            expect(rows).toEqual(CLAIMED);
            expect(tick).toBe(250);
            // The loop is intact: the next tick claims again.
            await tickAgain(scheduler, fake, 2);
            expect(fake.rearms()).toHaveLength(1);
        } finally {
            await provider.stop();
        }
    });

    it('writes nothing back when every dispatch succeeds', async () => {
        const fake = fakePool();
        const { provider, scheduler } = bound(fake.pool, () => Promise.resolve());
        try {
            scheduler.advance(250);
            await vi.waitFor(() => expect(fake.claims()).toHaveLength(1));
            await tickAgain(scheduler, fake, 2);
            expect(fake.rearms()).toHaveLength(0);
        } finally {
            await provider.stop();
        }
    });

    it('survives a throwing reporter and a failing re-arm', async () => {
        const error = vi.spyOn(console, 'error').mockImplementation(() => {});
        const fake = fakePool({ rearmFails: true });
        let reported = 0;
        const { provider, scheduler } = bound(
            fake.pool,
            () => Promise.reject(new Error('dispatch deadline')),
            () => {
                reported++;
                throw new Error('reporter broke');
            }
        );
        try {
            scheduler.advance(250);
            await vi.waitFor(() => expect(fake.rearms()).toHaveLength(1));
            // Collected before reporting, so the re-arm was still attempted
            // for both rows — and the reporter was still called per attempt.
            expect(reported).toBe(2);
            // The failed re-arm is logged, not thrown: the next tick runs.
            await vi.waitFor(() =>
                expect(
                    error.mock.calls.some((c) => String(c[0]).includes('could not re-arm'))
                ).toBe(true)
            );
            await tickAgain(scheduler, fake, 2);
        } finally {
            await provider.stop();
        }
    });
});
