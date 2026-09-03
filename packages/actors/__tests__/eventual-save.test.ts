import { afterEach, describe, expect, it, vi } from 'vitest';
import {
    defineActor,
    type ActorContext,
    type ActorStorage,
    type AnyActorDefinition,
    type StateErrorPhase
} from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage, type Host, type ManualScheduler } from '@sigx/actors/host';

/**
 * `ctx.save({ durability: 'eventual' })` on an EXPLICIT-persistence actor
 * (#320): the seam `job.checkpoint(cp, { durability })` passes through.
 * The job tests prove the job-level contract on a real clock; these drive
 * the debounce by hand so the timing claims are exact — the flush fires at
 * the window, a burst is one write, `clearState()` cancels what was
 * pending, and a failing flush reports through `onStateError('flush')`.
 */

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

type Seen = { key: string; error: unknown; phase: StateErrorPhase; n: number };

/** memoryStorage that counts `type`'s saves and rejects them while `failing.on`. */
function countingStorage(type: string): {
    storage: ActorStorage;
    inner: ActorStorage;
    counts: { saves: number };
    failing: { on: boolean };
} {
    const inner = memoryStorage();
    const counts = { saves: 0 };
    const failing = { on: false };
    return {
        inner,
        counts,
        failing,
        storage: {
            load: (t, k) => inner.load(t, k),
            save: (t, k, s, e) => {
                if (t === type) counts.saves++;
                if (failing.on) return Promise.reject(new Error('disk on fire'));
                return inner.save(t, k, s, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        }
    };
}

function explicitActor(type: string, seen: Seen[] | null) {
    const onStateError = (ctx: ActorContext<{ n: number }>, error: unknown, phase: StateErrorPhase) => {
        seen!.push({ key: ctx.key, error, phase, n: ctx.state.n });
    };
    return defineActor({
        type,
        allowAnonymous: true,
        state: () => ({ n: 0 }),
        // The default: only `ctx.save()` writes.
        persistence: 'explicit',
        ...(seen ? { onStateError } : {}),
        methods: (ctx) => ({
            async bump(durability: 'immediate' | 'eventual') {
                ctx.state.n++;
                await ctx.save({ durability });
                return ctx.state.n;
            },
            async wipe() {
                await ctx.clearState();
            }
        })
    });
}

async function stored(storage: ActorStorage, type: string): Promise<number | undefined> {
    return ((await storage.load(type, 'k'))?.state as { n: number } | undefined)?.n;
}

let running: Host | null = null;
let clock: ManualScheduler;
afterEach(async () => {
    vi.restoreAllMocks();
    await running?.stop({ timeoutMs: 1000 });
    running = null;
});

function start(def: AnyActorDefinition, storage: ActorStorage): Host {
    clock = manualScheduler();
    const host = createHost({ actors: [def], storage, scheduler: clock, defaults: quiet });
    running = host;
    return host;
}

describe("ctx.save({ durability: 'eventual' }) on an explicit-persistence actor (#320)", () => {
    it('resolves before the write, and the debounce flushes a burst as ONE save at the 50 ms window', async () => {
        const { storage, inner, counts } = countingStorage('Eventual');
        const def = explicitActor('Eventual', null);
        const client = start(def, storage).actor(def, 'k');
        // Five eventual saves resolve with nothing written yet.
        for (let i = 1; i <= 5; i++) expect(await client.bump('eventual')).toBe(i);
        expect(counts.saves).toBe(0);
        expect(await stored(inner, 'Eventual')).toBeUndefined();
        // Just short of the window: still nothing.
        clock.advance(49);
        await new Promise((r) => setTimeout(r, 0));
        expect(counts.saves).toBe(0);
        // The window closes: one write, carrying the whole burst.
        clock.advance(1);
        await vi.waitFor(() => expect(counts.saves).toBe(1));
        expect(await stored(inner, 'Eventual')).toBe(5);
        // The timer is one-shot: a later eventual save re-arms it.
        await client.bump('eventual');
        clock.advance(50);
        await vi.waitFor(() => expect(counts.saves).toBe(2));
        expect(await stored(inner, 'Eventual')).toBe(6);
    });

    it('works with `persistence` omitted — the default IS explicit, and most actors never spell it', async () => {
        const { storage, inner, counts } = countingStorage('Unspelled');
        const def = defineActor({
            type: 'Unspelled',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                    await ctx.save({ durability: 'eventual' });
                    return ctx.state.n;
                }
            })
        });
        const client = start(def, storage).actor(def, 'k');
        await expect(client.bump()).resolves.toBe(1);
        expect(counts.saves).toBe(0);
        clock.advance(50);
        await vi.waitFor(() => expect(counts.saves).toBe(1));
        expect(await stored(inner, 'Unspelled')).toBe(1);
    });

    it('an immediate save() carries a pending eventual one — no second write follows', async () => {
        const { storage, inner, counts } = countingStorage('Carried');
        const def = explicitActor('Carried', null);
        const client = start(def, storage).actor(def, 'k');
        await client.bump('eventual');
        await client.bump('immediate');
        expect(counts.saves).toBe(1);
        expect(await stored(inner, 'Carried')).toBe(2);
        // The armed debounce fires onto an already-saved version: no-op.
        clock.advance(50);
        await new Promise((r) => setTimeout(r, 0));
        expect(counts.saves).toBe(1);
    });

    it('deactivation flushes a pending eventual save — the one write it was asked for', async () => {
        const { storage, inner, counts } = countingStorage('Flushed');
        const def = explicitActor('Flushed', null);
        const host = start(def, storage);
        await host.actor(def, 'k').bump('eventual');
        expect(counts.saves).toBe(0);
        await host.deactivateType('Flushed');
        expect(counts.saves).toBe(1);
        expect(await stored(inner, 'Flushed')).toBe(1);
    });

    it('clearState() drops the pending eventual save: neither the debounce nor deactivation resurrects the record', async () => {
        const { storage, inner, counts } = countingStorage('Wiped');
        const def = explicitActor('Wiped', null);
        const host = start(def, storage);
        const client = host.actor(def, 'k');
        await client.bump('immediate');
        expect(await stored(inner, 'Wiped')).toBe(1);
        await client.bump('eventual');
        await client.wipe();
        expect(await stored(inner, 'Wiped')).toBeUndefined();
        // The debounce the eventual save armed must not write `state(key)`
        // back — an explicit actor writes nothing it was not asked for.
        clock.advance(50);
        await new Promise((r) => setTimeout(r, 0));
        expect(await stored(inner, 'Wiped')).toBeUndefined();
        await host.deactivateType('Wiped');
        expect(await stored(inner, 'Wiped')).toBeUndefined();
        expect(counts.saves).toBe(1);
    });

    it('a flush already queued behind the turn that clearState()s is a no-op — no resurrection in that window either', async () => {
        // The window: the debounce timer has FIRED (so there is nothing left
        // to cancel) and its system turn is queued behind a user turn that
        // goes on to clearState(). The flush must then write nothing.
        const { storage, inner, counts } = countingStorage('Queued');
        let release!: () => void;
        const held = new Promise<void>((r) => (release = r));
        const def = defineActor({
            type: 'Queued',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            persistence: 'explicit',
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                    await ctx.save({ durability: 'eventual' });
                },
                async wipeSlowly() {
                    await held;
                    await ctx.clearState();
                }
            })
        });
        clock = manualScheduler();
        const host = createHost({ actors: [def], storage, scheduler: clock, defaults: quiet });
        running = host;
        const client = host.actor(def, 'k');
        await client.bump();
        const wiping = client.wipeSlowly();
        await new Promise((r) => setTimeout(r, 0)); // the turn is in, parked on `held`
        clock.advance(50); // the timer fires; its flush turn queues behind it
        release();
        await wiping;
        await new Promise((r) => setTimeout(r, 0));
        expect(counts.saves).toBe(0);
        expect(await stored(inner, 'Queued')).toBeUndefined();
        await host.deactivateType('Queued');
        expect(counts.saves).toBe(0);
        expect(await stored(inner, 'Queued')).toBeUndefined();
    });

    it("a failing eventual flush reports through onStateError('flush'); the next save() carries the state", async () => {
        const seen: Seen[] = [];
        const { storage, inner, counts, failing } = countingStorage('Flaky');
        const def = explicitActor('Flaky', seen);
        const client = start(def, storage).actor(def, 'k');
        const error = vi.spyOn(console, 'error').mockImplementation(() => {});
        failing.on = true;
        await client.bump('eventual');
        clock.advance(50);
        await vi.waitFor(() => expect(seen).toHaveLength(1));
        expect(seen[0]).toEqual({ key: 'k', error: expect.any(Error), phase: 'flush', n: 1 });
        expect((seen[0]!.error as Error).message).toBe('disk on fire');
        // The hook owns reporting; the activation is still serving.
        expect(error).not.toHaveBeenCalled();
        // No retry of its own — an explicit actor has no dirty boundary to
        // re-arm the debounce — so the state stays unsaved until asked.
        expect(counts.saves).toBe(1);
        expect(await stored(inner, 'Flaky')).toBeUndefined();
        failing.on = false;
        await expect(client.bump('immediate')).resolves.toBe(2);
        expect(await stored(inner, 'Flaky')).toBe(2);
        expect(seen).toHaveLength(1);
    });

    it('without a hook, a failed eventual flush is dev-logged as a deferred save, not a write-behind flush', async () => {
        const { storage, failing } = countingStorage('Silent');
        const def = explicitActor('Silent', null);
        const client = start(def, storage).actor(def, 'k');
        const error = vi.spyOn(console, 'error').mockImplementation(() => {});
        failing.on = true;
        await client.bump('eventual');
        clock.advance(50);
        await vi.waitFor(() =>
            expect(error.mock.calls.flat().join(' ')).toMatch(/deferred save of Silent\/k failed/)
        );
        failing.on = false;
    });
});
