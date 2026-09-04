/**
 * The `ActorReminders` conformance suite: what "this is a reminder provider
 * the host can run on" means, as runnable cases (#385).
 *
 * It exists because four providers (`shardedReminders`, pg, SurrealDB, the
 * Durable Object alarm — and Redis makes five) had each re-pinned the same
 * seam in their own test file: the one-shot that fires once, the periodic
 * that advances before delivery and never bursts, the failed dispatch that
 * is re-armed one tick out and reported (#306, #326), the three
 * "meanwhile" rules. Two of those files shared their header word for word.
 * Copies drift, and every new provider started the archaeology over.
 *
 * **Assert the OUTCOME, never the mechanism.** Postgres claims with
 * `SKIP LOCKED`, SurrealDB partitions by shard, Redis runs a Lua script,
 * the sharded table CAS-saves a record, a Durable Object fires an alarm.
 * Nothing here may care: every case is phrased as what the host sees
 * through `deliver`, `undelivered` and the `ReminderApi`. In particular
 * `ownsShard` is never asserted — each provider decides what it means.
 *
 * The clock is REAL. Three providers judge "due" on a database clock the
 * suite cannot move, so every case sets reminders due now (`due: 0`) or
 * far out, drives the provider's TICK through a manual scheduler, and
 * sleeps `tickMs` of wall time where a re-arm has to become due. That is
 * why `tickMs` here is small (200 ms) and why a case waits for an outcome
 * rather than asserting it synchronously.
 *
 * What is deliberately NOT here:
 *
 *  - **`ownsShard` semantics.** `pgReminders` and `redisReminders` ignore
 *    it; `shardedReminders` and `surrealReminders` partition by it. Each
 *    provider pins its own posture.
 *  - **The one-provider-per-host rule** (`bind()` twice throws). A host
 *    invariant, tested with the host.
 *  - **Bytes and commands.** A provider's cost is a benchmark, not a
 *    conformance case.
 */
import { manualScheduler, type ManualScheduler } from '../host/scheduler';
import { memoryStorage } from '../host/storage-memory';
import type { ActorReminders, ActorRemindersContext, ActorRef, ActorStorage } from '../types';
import type { ConformanceCase, ConformanceSkip } from './conformance';

// ---------------------------------------------------------------------------
// The harness a provider supplies

export interface RemindersConformanceHarness {
    /**
     * OPTIONAL: this package's schema bootstrap (`ensurePgSchema`, …), run
     * ONCE before the provider is touched.
     */
    bootstrap?(): Promise<void>;
    /**
     * A provider over a table that is EMPTY when the harness is created.
     * The suite binds it and drives it. Called MORE THAN ONCE by the cases
     * that need two tickers or a restart, and every instance must see the
     * SAME table — a fresh schema, namespace or key prefix per harness,
     * never per call. `stop()` is the cleanup.
     */
    reminders(): ActorReminders;
    /** Drop the table and close every connection the factory opened. */
    stop(): Promise<void>;
    /**
     * `false` when the provider does not run a tick loop off the host's
     * scheduler — a Durable Object fires from a platform alarm. The
     * tick-driven cases then report a skip; the `ReminderApi` cases still
     * run. Default `true`.
     */
    tickDriven?: boolean;
    /**
     * `true` when one provider instance serves exactly ONE actor — a
     * Durable Object hosts one actor and refuses another identity. The
     * cross-actor case then reports a skip. Default `false`.
     */
    singleActor?: boolean;
}

export type RemindersConformanceFactory = () => Promise<RemindersConformanceHarness>;

// ---------------------------------------------------------------------------
// Assertions — deliberately framework-free

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(`[reminders conformance] ${message}`);
}

function show(value: unknown): string {
    try {
        return JSON.stringify(value) ?? String(value);
    } catch {
        return String(value);
    }
}

const NUL = '\u0000';
/** Small, so a case that must sleep a tick of wall time stays quick. */
const TICK_MS = 200;
/** Past what a real store needs to answer a delivery's follow-up calls. */
const SETTLE_MS = 150;
const WAIT_MS = 5_000;

const sleep = (ms: number): Promise<void> => new Promise((resolve) => setTimeout(resolve, ms));

/** Poll until `check` stops throwing, or fail with its last complaint. */
async function until(check: () => void | Promise<void>, what: string): Promise<void> {
    const deadline = Date.now() + WAIT_MS;
    let last: unknown = null;
    for (;;) {
        try {
            await check();
            return;
        } catch (error) {
            last = error;
        }
        if (Date.now() > deadline) {
            throw new Error(
                `[reminders conformance] ${what}: still not true after ${WAIT_MS} ms — ` +
                    `${last instanceof Error ? last.message : show(last)}`
            );
        }
        await sleep(25);
    }
}

interface Delivery {
    ref: ActorRef;
    name: string;
}
interface Failure extends Delivery {
    error: unknown;
}

/**
 * A provider bound to a manual scheduler and a recording `deliver`. `fail`
 * decides per attempt whether the dispatch rejects (it may also act on the
 * api first — the "meanwhile" cases do); every attempt is recorded in
 * `delivered`, every failure in `undelivered`.
 */
interface Bound {
    provider: ActorReminders;
    scheduler: ManualScheduler;
    delivered: Delivery[];
    undelivered: Failure[];
    /** Advance the scheduler by one tick. */
    tick(): void;
    stop(): Promise<void>;
}

function bind(
    harness: RemindersConformanceHarness,
    storage: ActorStorage,
    fail: (ref: ActorRef, name: string) => boolean | Promise<boolean> = () => false
): Bound {
    const scheduler = manualScheduler();
    const delivered: Delivery[] = [];
    const undelivered: Failure[] = [];
    const provider = harness.reminders();
    const context: ActorRemindersContext = {
        storage,
        scheduler,
        tickMs: TICK_MS,
        ownsShard: () => true,
        deliver: async (ref, name) => {
            delivered.push({ ref, name });
            if (await fail(ref, name)) throw new Error('dispatch deadline');
        },
        undelivered: (ref, name, error) => void undelivered.push({ ref, name, error })
    };
    provider.bind(context);
    return {
        provider,
        scheduler,
        delivered,
        undelivered,
        tick: () => scheduler.advance(TICK_MS),
        stop: async () => {
            await provider.stop();
        }
    };
}

/** The API cases run everywhere; the tick cases need a loop to drive. */
function tickCase(
    name: string,
    why: string,
    run: (harness: RemindersConformanceHarness, storage: ActorStorage) => Promise<void>
): ConformanceCase<RemindersConformanceFactory> {
    return {
        name,
        why,
        async run(create): Promise<void | ConformanceSkip> {
            const harness = await create();
            try {
                await harness.bootstrap?.();
                if (harness.tickDriven === false) {
                    return { skipped: 'the provider fires from a platform alarm, not a scheduler tick' };
                }
                // One storage per case: the sharded provider keeps its
                // table HERE, so two tickers or a restart must share it.
                await run(harness, memoryStorage());
            } finally {
                await harness.stop();
            }
        }
    };
}

function apiCase(
    name: string,
    why: string,
    run: (harness: RemindersConformanceHarness, storage: ActorStorage) => Promise<void>
): ConformanceCase<RemindersConformanceFactory> {
    return {
        name,
        why,
        async run(create): Promise<void> {
            const harness = await create();
            try {
                await harness.bootstrap?.();
                await run(harness, memoryStorage());
            } finally {
                await harness.stop();
            }
        }
    };
}

/** A firing due at the next tick; the host passes "ms from now". */
const NOW = { due: 0 };
const FAR = 3_600_000;
const PERIOD = 60_000;

// ---------------------------------------------------------------------------
// The cases

export const remindersConformance: readonly ConformanceCase<RemindersConformanceFactory>[] = [
    apiCase(
        'set/list/clear round-trip, with NUL and backslashes in the ref and name',
        'a reminder set under an odd key must be listed and cleared under exactly that key, or an actor with a NUL in its key silently loses its wake',
        async (harness, storage) => {
            const { provider, stop } = bind(harness, storage);
            try {
                const ref = { type: 'Room', key: `general${NUL}with\\slash` };
                const api = provider.apiFor(ref);
                await api.set('cleanup', { due: FAR });
                await api.set(`odd${NUL}name`, { due: FAR, period: PERIOD });
                const names = (await api.list()).sort();
                assert(
                    names.length === 2 && names[0] === 'cleanup' && names[1] === `odd${NUL}name`,
                    `list() after two sets is ${show(names)}`
                );
                await api.clear('cleanup');
                await api.clear(`odd${NUL}name`);
                const after = await api.list();
                assert(after.length === 0, `list() after clears is ${show(after)}`);
            } finally {
                await stop();
            }
        }
    ),

    {
        name: 'reminders of one actor are invisible to another',
        why: 'a key that differs by one character is a different actor; a provider that keyed by type alone, or split a NUL-bearing key wrongly, would list one actor’s reminders on another',
        async run(create): Promise<void | ConformanceSkip> {
            const harness = await create();
            try {
                await harness.bootstrap?.();
                if (harness.singleActor) return { skipped: 'one actor per provider instance' };
                const { provider, stop } = bind(harness, memoryStorage());
                try {
                    const a = provider.apiFor({ type: 'Room', key: `general${NUL}x` });
                    const b = provider.apiFor({ type: 'Room', key: 'general' });
                    const c = provider.apiFor({ type: 'Hall', key: 'general' });
                    await a.set('cleanup', { due: FAR });
                    const others = [...(await b.list()), ...(await c.list())];
                    assert(others.length === 0, `a different actor lists ${show(others)}, expected nothing`);
                    await b.set('cleanup', { due: FAR });
                    await a.clear('cleanup');
                    const left = await b.list();
                    assert(left.length === 1, `clearing one actor’s reminder removed another’s: ${show(left)}`);
                } finally {
                    await stop();
                }
            } finally {
                await harness.stop();
            }
        }
    },

    apiCase(
        'set of an existing name overwrites it',
        'a second set is a later decision — the old due time or period must not survive it',
        async (harness, storage) => {
            const { provider, stop } = bind(harness, storage);
            try {
                const api = provider.apiFor({ type: 'T', key: 'k' });
                await api.set('x', { due: FAR, period: PERIOD });
                await api.set('x', { due: FAR });
                const names = await api.list();
                assert(names.length === 1 && names[0] === 'x', `list() after re-set is ${show(names)}`);
            } finally {
                await stop();
            }
        }
    ),

    apiCase(
        'rejects periods under the 60 s floor',
        'anything tighter is a timer’s job; a provider that accepts it turns the tick loop into a hot loop',
        async (harness, storage) => {
            const { provider, stop } = bind(harness, storage);
            try {
                let rejected = false;
                try {
                    await provider.apiFor({ type: 'T', key: 'k' }).set('fast', { due: 0, period: 1_000 });
                } catch {
                    rejected = true;
                }
                assert(rejected, 'set() with a 1 s period resolved, expected a rejection');
            } finally {
                await stop();
            }
        }
    ),

    apiCase(
        'reminders survive a provider restart',
        'a reminder lives in the store, not in the provider instance — a host restart must find it',
        async (harness, storage) => {
            const a = bind(harness, storage);
            const ref = { type: 'Durable', key: 'k' };
            await a.provider.apiFor(ref).set('wake', { due: FAR });
            await a.stop();
            const b = bind(harness, storage);
            try {
                const names = await b.provider.apiFor(ref).list();
                assert(names.length === 1 && names[0] === 'wake', `after a restart list() is ${show(names)}`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'a one-shot fires once and is gone before delivery',
        'the entry is advanced or deleted BEFORE dispatch, so a crash mid-dispatch skips a firing rather than double-firing; a one-shot still listed during its own delivery would fire again',
        async (harness, storage) => {
            let listedDuringDelivery: string[] | null = null;
            const b = bind(harness, storage, async (ref) => {
                listedDuringDelivery = await b.provider.apiFor(ref).list();
                return false;
            });
            try {
                const ref = { type: 'Shot', key: `k${NUL}1` };
                await b.provider.apiFor(ref).set('once', NOW);
                b.provider.start();
                b.tick();
                await until(() => assert(b.delivered.length === 1, `delivered ${b.delivered.length}`), 'the one-shot fires');
                assert(
                    b.delivered[0]!.ref.type === ref.type && b.delivered[0]!.ref.key === ref.key && b.delivered[0]!.name === 'once',
                    `delivered ${show(b.delivered[0])}`
                );
                assert(
                    listedDuringDelivery !== null && (listedDuringDelivery as string[]).length === 0,
                    `list() during delivery was ${show(listedDuringDelivery)}, expected [] (gone before dispatch)`
                );
                // Settle first: a single-flight loop still inside its
                // first tick would skip a tick fired on its heels, and a
                // skipped tick is indistinguishable from an empty one.
                await sleep(SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                assert(b.delivered.length === 1, `delivered ${b.delivered.length} times, expected exactly once`);
                assert(b.undelivered.length === 0, `${b.undelivered.length} undelivered reports for a clean delivery`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'a periodic reminder advances before delivery and never bursts',
        'a periodic entry that is not advanced before dispatch fires every tick until the dispatch settles; one that replays missed periods after downtime floods the actor',
        async (harness, storage) => {
            const b = bind(harness, storage);
            try {
                const ref = { type: 'Beat', key: 'k' };
                await b.provider.apiFor(ref).set('pulse', { due: 0, period: PERIOD });
                b.provider.start();
                b.tick();
                await until(() => assert(b.delivered.length === 1, `delivered ${b.delivered.length}`), 'the periodic fires');
                const names = await b.provider.apiFor(ref).list();
                assert(names.length === 1 && names[0] === 'pulse', `still registered? list() is ${show(names)}`);
                // Already a period out: a second and a third tick find
                // nothing, however many ticks pass within the period. (Settled
                // between, so the single-flight loop cannot skip them.)
                await sleep(SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                assert(b.delivered.length === 1, `delivered ${b.delivered.length} times within one period`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'two providers ticking the same table deliver a due reminder exactly once',
        'two hosts always tick concurrently; a provider that lets both claim the same entry double-fires every reminder in the cluster',
        async (harness, storage) => {
            const a = bind(harness, storage);
            const b = bind(harness, storage);
            try {
                const ref = { type: 'Once', key: 'contended' };
                await a.provider.apiFor(ref).set('only', NOW);
                a.provider.start();
                b.provider.start();
                a.tick();
                b.tick();
                await until(
                    () => assert(a.delivered.length + b.delivered.length >= 1, 'nothing delivered'),
                    'one of the tickers fires'
                );
                await sleep(SETTLE_MS * 2);
                const total = a.delivered.length + b.delivered.length;
                assert(total === 1, `delivered ${total} times across two tickers, expected exactly once`);
            } finally {
                await a.stop();
                await b.stop();
            }
        }
    ),

    tickCase(
        'a one-shot whose dispatch failed is re-armed one tick out and reported',
        'a deadline or a restarting host must cost one tick, not the wake (#306) — and the host counts the attempt through `undelivered`',
        async (harness, storage) => {
            let fail = true;
            const b = bind(harness, storage, () => fail);
            try {
                const ref = { type: 'Retry', key: `shot${NUL}1` };
                const api = b.provider.apiFor(ref);
                await api.set('wake', NOW);
                b.provider.start();
                b.tick();
                await until(() => assert(b.delivered.length === 1, `delivered ${b.delivered.length}`), 'the first attempt');
                await until(() => assert(b.undelivered.length === 1, `undelivered ${b.undelivered.length}`), 'the failure is reported');
                assert(
                    b.undelivered[0]!.name === 'wake' && b.undelivered[0]!.ref.key === ref.key,
                    `reported ${show(b.undelivered[0])}`
                );
                assert(
                    (b.undelivered[0]!.error as Error).message === 'dispatch deadline',
                    `reported error is ${show(b.undelivered[0]!.error)}, expected the dispatch’s own`
                );
                // Still registered — one tick out, not dropped.
                await until(async () => {
                    const names = await api.list();
                    assert(names.length === 1 && names[0] === 'wake', `list() is ${show(names)}`);
                }, 'the one-shot is re-armed');
                // Not sooner than a tick: a re-tick before one has elapsed
                // finds nothing.
                await sleep(SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                assert(b.delivered.length === 1, `retried within the same tick (${b.delivered.length} attempts)`);
                fail = false;
                await sleep(TICK_MS);
                b.tick();
                await until(() => assert(b.delivered.length === 2, `delivered ${b.delivered.length}`), 'the retry lands');
                await until(async () => {
                    const names = await api.list();
                    assert(names.length === 0, `list() after the retry fired is ${show(names)}`);
                }, 'a one-shot that finally fired clears itself');
                assert(b.undelivered.length === 1, `${b.undelivered.length} failures reported, expected 1`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'a periodic reminder whose dispatch failed is retried next tick, then resumes its cadence',
        'the retry must be pulled forward to the next tick, not left a whole period out — and a successful firing must advance by the period again, not keep retrying every tick',
        async (harness, storage) => {
            let fail = true;
            const b = bind(harness, storage, () => fail);
            try {
                const ref = { type: 'Retry', key: 'beat' };
                const api = b.provider.apiFor(ref);
                await api.set('beat', { due: 0, period: PERIOD });
                b.provider.start();
                b.tick();
                await until(() => assert(b.undelivered.length === 1, `undelivered ${b.undelivered.length}`), 'the failure is reported');
                fail = false;
                await sleep(TICK_MS);
                b.tick();
                await until(() => assert(b.delivered.length === 2, `delivered ${b.delivered.length}`), 'the retry lands next tick');
                // Cadence resumed: the next firing is a period out.
                await sleep(TICK_MS);
                b.tick();
                await sleep(SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                assert(b.delivered.length === 2, `delivered ${b.delivered.length} times, expected the cadence to resume`);
                const names = await api.list();
                assert(names.length === 1, `a periodic that fired is gone: list() is ${show(names)}`);
                assert(b.undelivered.length === 1, `${b.undelivered.length} failures reported, expected 1`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'a permanently failing target costs one attempt per tick, each reported',
        'a target that never answers must cost one attempt per tick, never a hot loop — and every attempt must reach the host’s counter',
        async (harness, storage) => {
            const b = bind(harness, storage, () => true);
            try {
                const ref = { type: 'Retry', key: 'never' };
                const api = b.provider.apiFor(ref);
                await api.set('wake', NOW);
                b.provider.start();
                for (let attempt = 1; attempt <= 3; attempt++) {
                    b.tick();
                    await until(() => assert(b.delivered.length === attempt, `delivered ${b.delivered.length}`), `attempt ${attempt}`);
                    await sleep(SETTLE_MS);
                    assert(b.delivered.length === attempt, `a hot loop: ${b.delivered.length} attempts after tick ${attempt}`);
                    await sleep(TICK_MS);
                }
                assert(b.undelivered.length === 3, `${b.undelivered.length} failures reported, expected 3`);
                const names = await api.list();
                assert(names.length === 1 && names[0] === 'wake', `still registered? list() is ${show(names)}`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'a reminder the actor set again during its failing dispatch is left as the actor set it',
        'the actor made a later decision (typically `onReminder` rescheduling itself before the dispatch timed out) and that decision wins; a re-arm that overwrote it would fire an hour-out reminder next tick',
        async (harness, storage) => {
            const ref = { type: 'Retry', key: 'meanwhile-set' };
            const b = bind(harness, storage, async (_ref, name) => {
                if (name === 'wake') await b.provider.apiFor(ref).set('wake', { due: FAR });
                return true;
            });
            try {
                const api = b.provider.apiFor(ref);
                await api.set('wake', NOW);
                b.provider.start();
                b.tick();
                await until(() => assert(b.undelivered.length === 1, `undelivered ${b.undelivered.length}`), 'the failure is reported');
                // Give the re-arm time to land, then prove it did not win.
                await sleep(TICK_MS + SETTLE_MS);
                b.tick();
                await sleep(TICK_MS + SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                assert(b.delivered.length === 1, `delivered ${b.delivered.length} times — the re-arm overrode the actor’s far-out set`);
                const names = await api.list();
                assert(names.length === 1 && names[0] === 'wake', `list() is ${show(names)}`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'a periodic reminder the actor cleared during its failing dispatch stays cleared',
        'the clear removed the advanced entry; a re-arm that resurrected it would fire a reminder the actor no longer wants, every period, forever',
        async (harness, storage) => {
            const ref = { type: 'Retry', key: 'meanwhile-clear' };
            const b = bind(harness, storage, async (_ref, name) => {
                if (name === 'beat') await b.provider.apiFor(ref).clear('beat');
                return true;
            });
            try {
                const api = b.provider.apiFor(ref);
                await api.set('beat', { due: 0, period: PERIOD });
                b.provider.start();
                b.tick();
                await until(() => assert(b.undelivered.length === 1, `undelivered ${b.undelivered.length}`), 'the failure is reported');
                await sleep(TICK_MS + SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                assert(b.delivered.length === 1, `delivered ${b.delivered.length} times — a cleared periodic was resurrected`);
                const names = await api.list();
                assert(names.length === 0, `list() is ${show(names)}, expected the clear to hold`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'a one-shot the actor cleared during its failing dispatch is retried at most once',
        'the tick had already deleted it, so the clear left nothing behind and absent is indistinguishable from untouched — documented: one extra delivery is allowed (onReminder must be idempotent), a reminder that keeps coming back is not',
        async (harness, storage) => {
            const ref = { type: 'Retry', key: 'meanwhile-clear-shot' };
            let attempts = 0;
            const b = bind(harness, storage, async (_ref, name) => {
                // Only the FIRST attempt clears and times out — the shape
                // of an `onReminder` that cleared itself just before its
                // dispatch's deadline. A retry, if any, succeeds.
                if (name === 'once' && attempts++ === 0) {
                    await b.provider.apiFor(ref).clear('once');
                    return true;
                }
                return false;
            });
            try {
                const api = b.provider.apiFor(ref);
                await api.set('once', NOW);
                b.provider.start();
                b.tick();
                await until(() => assert(b.undelivered.length === 1, `undelivered ${b.undelivered.length}`), 'the failure is reported');
                await sleep(TICK_MS + SETTLE_MS);
                b.tick();
                await sleep(TICK_MS + SETTLE_MS);
                b.tick();
                await sleep(SETTLE_MS);
                assert(
                    b.delivered.length <= 2,
                    `delivered ${b.delivered.length} times, expected at most one retry`
                );
                const names = await api.list();
                assert(names.length === 0, `list() is ${show(names)}, expected nothing left`);
            } finally {
                await b.stop();
            }
        }
    ),

    tickCase(
        'a deliver() that throws synchronously is retried and reported like a rejection',
        'the context is pluggable; a provider that only catches rejections loses the wake when `deliver` throws before it returns a promise',
        async (harness, storage) => {
            const scheduler = manualScheduler();
            const delivered: Delivery[] = [];
            const undelivered: Failure[] = [];
            const provider = harness.reminders();
            let attempts = 0;
            provider.bind({
                storage,
                scheduler,
                tickMs: TICK_MS,
                ownsShard: () => true,
                deliver: (ref, name): Promise<unknown> => {
                    delivered.push({ ref, name });
                    if (attempts++ === 0) throw new Error('sync throw');
                    return Promise.resolve();
                },
                undelivered: (ref, name, error) => void undelivered.push({ ref, name, error })
            });
            try {
                const api = provider.apiFor({ type: 'Sync', key: 'k' });
                await api.set('wake', NOW);
                provider.start();
                scheduler.advance(TICK_MS);
                await until(() => assert(undelivered.length === 1, `undelivered ${undelivered.length}`), 'the throw is reported');
                assert((undelivered[0]!.error as Error).message === 'sync throw', `reported ${show(undelivered[0]!.error)}`);
                await sleep(TICK_MS);
                scheduler.advance(TICK_MS);
                await until(() => assert(delivered.length === 2, `delivered ${delivered.length}`), 'the retry lands');
                await until(async () => assert((await api.list()).length === 0, 'still listed'), 'the one-shot clears');
            } finally {
                await provider.stop();
            }
        }
    )
];
