/**
 * The shared `ActorReminders` conformance suite (#385), run against the
 * default `shardedReminders()` — the incumbent, whose behaviour IS the
 * contract the other providers were written to.
 *
 * The second half is the suite's own proof: a deliberately broken
 * in-memory provider with one bug switched on at a time, and the case that
 * is supposed to catch that bug must go red. A conformance case that cannot
 * fail is decoration (AGENTS.md), so every bug here names its case.
 */
import { describe, expect, it } from 'vitest';
import type { ActorReminders, ActorRemindersContext, ActorRef, ReminderApi } from '@sigx/actors';
import { memoryStorage, shardedReminders } from '@sigx/actors/host';
import {
    remindersConformance,
    type RemindersConformanceFactory,
    type RemindersConformanceHarness
} from '@sigx/actors/testing';

describe('remindersConformance × shardedReminders', () => {
    const create: RemindersConformanceFactory = async (): Promise<RemindersConformanceHarness> => ({
        // The sharded table lives in the context's storage, which the suite
        // shares across every provider it binds within a case — so two
        // tickers and a restart see one table with no help from here.
        reminders: () => shardedReminders(),
        stop: async () => {}
    });

    for (const c of remindersConformance) {
        it(c.name, async () => {
            const outcome = await c.run(create);
            expect(outcome).toBeUndefined();
        }, 20_000);
    }
});

// ---------------------------------------------------------------------------
// The red proof

type Bug =
    | 'one-shot-not-deleted'
    | 'periodic-not-advanced'
    | 'no-rearm'
    | 'rearm-immediately'
    | 'no-undelivered'
    | 'rearm-overrides-set'
    | 'rearm-resurrects-periodic'
    | 'no-floor'
    | 'not-durable'
    | 'sync-throw-lost'
    | 'no-atomic-claim';

interface Entry {
    nextDue: number;
    period?: number;
}

/**
 * A naive in-memory provider over a table the harness shares, correct
 * except for the one `bug` switched on. Correct means: the table is
 * advanced or deleted before delivery, a failed dispatch re-arms one tick
 * out under the "meanwhile" rules, and the floor is enforced.
 */
function brokenReminders(table: Map<string, Entry>, bug: Bug | null): ActorReminders {
    let ctx: ActorRemindersContext | null = null;
    let cancel: (() => void) | null = null;
    // `not-durable`: each instance keeps its own table.
    const own = bug === 'not-durable' ? new Map<string, Entry>() : table;
    const idOf = (ref: ActorRef, name: string): string => JSON.stringify([ref.type, ref.key, name]);
    const parse = (id: string): { ref: ActorRef; name: string } => {
        const [type, key, name] = JSON.parse(id) as [string, string, string];
        return { ref: { type, key }, name };
    };
    let claiming = false;
    const tick = async (): Promise<void> => {
        const context = ctx!;
        const now = Date.now();
        // `no-atomic-claim`: two tickers can both collect the same entry —
        // the deletion happens only after an await.
        if (bug === 'no-atomic-claim') {
            const due = [...own.entries()].filter(([, e]) => e.nextDue <= now);
            await new Promise((r) => setTimeout(r, 30));
            for (const [id] of due) own.delete(id);
            await Promise.allSettled(due.map(([id]) => context.deliver(parse(id).ref, parse(id).name)));
            return;
        }
        if (claiming) return;
        claiming = true;
        try {
            const due: { id: string; advanced: Entry | null }[] = [];
            for (const [id, entry] of own) {
                if (entry.nextDue > now) continue;
                if (entry.period !== undefined) {
                    if (bug !== 'periodic-not-advanced') {
                        let next = entry.nextDue + entry.period;
                        if (next <= now) next = now + entry.period;
                        entry.nextDue = next;
                    }
                    due.push({ id, advanced: { ...entry } });
                } else {
                    if (bug !== 'one-shot-not-deleted') own.delete(id);
                    due.push({ id, advanced: null });
                }
            }
            const failed: typeof due = [];
            await Promise.allSettled(
                due.map(async (d) => {
                    const { ref, name } = parse(d.id);
                    // `sync-throw-lost`: the call is made OUTSIDE the try, so
                    // only a rejection is caught and a throw escapes the
                    // tick uncounted.
                    const pending = bug === 'sync-throw-lost' ? context.deliver(ref, name) : null;
                    try {
                        await (pending ?? context.deliver(ref, name));
                    } catch (error) {
                        failed.push(d);
                        if (bug !== 'no-undelivered') context.undelivered?.(ref, name, error);
                    }
                })
            );
            if (bug === 'no-rearm') return;
            const nextDue = bug === 'rearm-immediately' ? Date.now() : Date.now() + context.tickMs;
            for (const { id, advanced } of failed) {
                const current = own.get(id);
                if (advanced === null) {
                    if (current !== undefined && bug !== 'rearm-overrides-set') continue;
                    own.set(id, { nextDue });
                } else if (current !== undefined) {
                    if (current.nextDue === advanced.nextDue && current.period === advanced.period) {
                        current.nextDue = Math.min(current.nextDue, nextDue);
                    } else if (bug === 'rearm-overrides-set') {
                        current.nextDue = nextDue;
                    }
                } else if (bug === 'rearm-resurrects-periodic') {
                    own.set(id, { nextDue, period: advanced.period });
                }
            }
        } finally {
            claiming = false;
        }
    };
    return {
        bind(context) {
            ctx = context;
        },
        start() {
            if (cancel) return;
            cancel = ctx!.scheduler.every(ctx!.tickMs, () => {
                if (bug === 'sync-throw-lost') {
                    // A synchronous throw inside the tick kills it silently.
                    void tick().catch(() => {});
                } else {
                    void tick();
                }
            });
        },
        stop() {
            cancel?.();
            cancel = null;
        },
        apiFor(ref): ReminderApi {
            return {
                async set(name, opts) {
                    if (bug !== 'no-floor' && opts.period !== undefined && opts.period < 60_000) {
                        throw new Error('period under the floor');
                    }
                    own.set(idOf(ref, name), {
                        nextDue: Date.now() + opts.due,
                        ...(opts.period !== undefined ? { period: opts.period } : {})
                    });
                },
                async clear(name) {
                    own.delete(idOf(ref, name));
                },
                async list() {
                    return [...own.keys()]
                        .map(parse)
                        .filter((p) => p.ref.type === ref.type && p.ref.key === ref.key)
                        .map((p) => p.name);
                }
            };
        }
    };
}

const CATCHES: Record<Bug, string> = {
    'one-shot-not-deleted': 'a one-shot fires once and is gone before delivery',
    'periodic-not-advanced': 'a periodic reminder advances before delivery and never bursts',
    'no-atomic-claim': 'two providers ticking the same table deliver a due reminder exactly once',
    'no-rearm': 'a one-shot whose dispatch failed is re-armed one tick out and reported',
    'rearm-immediately': 'a one-shot whose dispatch failed is re-armed one tick out and reported',
    'no-undelivered': 'a one-shot whose dispatch failed is re-armed one tick out and reported',
    'rearm-overrides-set': 'a reminder the actor set again during its failing dispatch is left as the actor set it',
    'rearm-resurrects-periodic': 'a periodic reminder the actor cleared during its failing dispatch stays cleared',
    'no-floor': 'rejects periods under the 60 s floor',
    'not-durable': 'reminders survive a provider restart',
    'sync-throw-lost': 'a deliver() that throws synchronously is retried and reported like a rejection'
};

describe('remindersConformance catches a broken provider', () => {
    it('the correct naive provider passes every case (the control)', async () => {
        const table = new Map<string, Entry>();
        const create: RemindersConformanceFactory = async () => ({
            reminders: () => brokenReminders(table, null),
            stop: async () => table.clear()
        });
        for (const c of remindersConformance) {
            await expect(c.run(create), c.name).resolves.toBeUndefined();
        }
    }, 60_000);

    for (const [bug, caseName] of Object.entries(CATCHES) as [Bug, string][]) {
        it(`${bug} → "${caseName}" goes red`, async () => {
            const table = new Map<string, Entry>();
            const create: RemindersConformanceFactory = async () => ({
                reminders: () => brokenReminders(table, bug),
                stop: async () => table.clear()
            });
            const c = remindersConformance.find((candidate) => candidate.name === caseName);
            expect(c, caseName).toBeDefined();
            await expect(c!.run(create)).rejects.toThrow(/\[reminders conformance\]/);
        }, 20_000);
    }
});

// Keep the storage import honest: the suite shares ONE storage per case,
// and this file's sharded harness relies on that rather than on its own.
void memoryStorage;
