/**
 * #326 — a reminder dispatch that fails is reported and re-armed, not lost.
 *
 * Not env-gated: the claim is answered by a fake `db`, which the structural
 * `SurrealQueryable` seam exists to allow, so this runs on every leg. What
 * the SurrealQL actually does to the rows is proven against a real SurrealDB
 * in `surreal-reminders.test.ts`; this file pins the control flow around it —
 * every failed attempt (a rejection AND a synchronous throw) reaches
 * `context.undelivered`, the batch's failures go back in ONE transaction
 * carrying the claim's own `at`, a clean batch writes nothing, and neither a
 * throwing reporter nor a failing re-arm can kill the tick loop.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { RecordId } from 'surrealdb';
import type { ActorRef } from '@sigx/actors';
import { manualScheduler, memoryStorage } from '@sigx/actors/host';
import { surrealReminders } from '../src';
import type { SurrealQueryable } from '../src/connection';

const isClaim = (query: string): boolean =>
    query.includes('RETURN { at: <string>$at, due: $due }');
const isRearm = (query: string): boolean => query.includes('FOR $f IN $failed');

const AT = '2026-09-02T10:00:00.123456789Z';
/** Two claimed rows: a periodic one (advanced) and a one-shot (deleted). */
const CLAIMED = [
    {
        id: new RecordId('sigx_reminder', ['Beat', 'k', 'beat']),
        t: 'Beat',
        k: 'k',
        n: 'beat',
        tk: 'Beat k',
        sh: 'p3',
        p: 60_000
    },
    {
        id: new RecordId('sigx_reminder', ['Shot', 'k', 'wake']),
        t: 'Shot',
        k: 'k',
        n: 'wake',
        tk: 'Shot k',
        sh: 'p9',
        p: 0
    }
];

function fakeDb(options: { rearmFails?: boolean } = {}) {
    const queries: { query: string; bindings: Record<string, unknown> | undefined }[] = [];
    let claims = 0;
    const db: SurrealQueryable = {
        query<R extends unknown[]>(query: string, bindings?: Record<string, unknown>): Promise<R> {
            queries.push({ query, bindings });
            if (isClaim(query)) {
                // Due once; every later tick finds nothing. One entry per
                // statement: BEGIN, LET, LET, UPDATE, DELETE, RETURN, COMMIT.
                const due = claims++ === 0 ? CLAIMED : [];
                return Promise.resolve([
                    null,
                    null,
                    null,
                    null,
                    null,
                    { at: AT, due },
                    null
                ] as unknown as R);
            }
            if (isRearm(query) && options.rearmFails) {
                return Promise.reject(new Error('connection reset'));
            }
            return Promise.resolve([] as unknown as R);
        }
    };
    return {
        db,
        claims: () => queries.filter((q) => isClaim(q.query)),
        rearms: () => queries.filter((q) => isRearm(q.query))
    };
}

function bound(
    db: SurrealQueryable,
    deliver: (ref: ActorRef, name: string) => Promise<unknown>,
    undelivered?: (ref: ActorRef, name: string, error: unknown) => void
) {
    const scheduler = manualScheduler();
    const provider = surrealReminders({ db });
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

describe('surrealReminders: a dispatch that fails (#326)', () => {
    it('reports every failed attempt and re-arms the batch in one transaction', async () => {
        vi.spyOn(console, 'error').mockImplementation(() => {});
        const fake = fakeDb();
        const undelivered: { ref: ActorRef; name: string; error: unknown }[] = [];
        const { provider, scheduler } = bound(
            fake.db,
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
            // One transaction for the batch: the failed rows as claimed (in
            // settlement order — the synchronous throw lands first), the
            // instant the claim advanced them from (so the re-arm can tell a
            // row from one the actor has since re-set), and the tick.
            const [rearm] = fake.rearms();
            const { at, failed, tick } = rearm!.bindings as {
                at: string;
                failed: (typeof CLAIMED)[number][];
                tick: number;
            };
            expect(at).toBe(AT);
            expect(tick).toBe(250);
            expect([...failed].sort((x, y) => x.n.localeCompare(y.n))).toEqual(CLAIMED);
            // The loop is intact: the next tick claims again.
            await tickAgain(scheduler, fake, 2);
            expect(fake.rearms()).toHaveLength(1);
        } finally {
            await provider.stop();
        }
    });

    it('writes nothing back when every dispatch succeeds', async () => {
        const fake = fakeDb();
        const { provider, scheduler } = bound(fake.db, () => Promise.resolve());
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
        const fake = fakeDb({ rearmFails: true });
        let reported = 0;
        const { provider, scheduler } = bound(
            fake.db,
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
