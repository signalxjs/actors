import { describe, expect, it, vi } from 'vitest';
import {
    defineActor,
    isActorError,
    type ActorContext,
    type ActorStorage,
    type DeactivationReason,
    type StateErrorPhase
} from '@sigx/actors';
import { createHost, memoryStorage } from '@sigx/actors/host';

/**
 * `onStateError` (#54): a write-behind save has no caller to throw to. The
 * two sites that can lose a write silently — the debounced flush and the
 * final flush at deactivation — hand the failure to the hook, so an app can
 * page, count, or dead-letter the state without polling for faults.
 */

const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** memoryStorage whose `save` rejects while `failing.on` is set. */
function flakyStorage(): { storage: ActorStorage; failing: { on: boolean; saves: number } } {
    const inner = memoryStorage();
    const failing = { on: false, saves: 0 };
    return {
        failing,
        storage: {
            load: (t, k) => inner.load(t, k),
            save: (t, k, s, e) => {
                failing.saves++;
                if (failing.on) return Promise.reject(new Error('disk on fire'));
                return inner.save(t, k, s, e);
            },
            clear: (t, k, e) => inner.clear(t, k, e)
        }
    };
}

type Seen = { key: string; error: unknown; phase: StateErrorPhase; n: number };

function wbActor(
    type: string,
    debounceMs: number,
    seen: Seen[],
    hook = true,
    deactivated: DeactivationReason[] = []
) {
    const onStateError = (
        ctx: ActorContext<{ n: number }>,
        error: unknown,
        phase: StateErrorPhase
    ) => {
        seen.push({ key: ctx.key, error, phase, n: ctx.state.n });
    };
    return defineActor({
        type,
        allowAnonymous: true,
        state: () => ({ n: 0 }),
        persistence: { mode: 'write-behind', debounceMs },
        ...(hook ? { onStateError } : {}),
        onDeactivate(_ctx, reason) {
            deactivated.push(reason);
        },
        methods: (ctx) => ({
            async bump() {
                ctx.state.n++;
                return ctx.state.n;
            }
        })
    });
}

describe('onStateError', () => {
    it('fires from the final write-behind flush at deactivation, then deactivation completes', async () => {
        const seen: Seen[] = [];
        const wb = wbActor('WBFinal', 60_000, seen);
        const { storage, failing } = flakyStorage();
        const error = vi.spyOn(console, 'error').mockImplementation(() => {});
        try {
            const host = createHost({ actors: [wb], storage, defaults: quiet });
            await host.actor(wb, 'w').bump();
            failing.on = true;
            await expect(host.deactivateType('WBFinal')).resolves.toBeUndefined();
            expect(seen).toEqual([
                { key: 'w', error: expect.any(Error), phase: 'final-flush', n: 1 }
            ]);
            expect((seen[0]!.error as Error).message).toBe('disk on fire');
            // The hook OWNS reporting — no dev log on top of it.
            expect(error).not.toHaveBeenCalled();
        } finally {
            error.mockRestore();
        }
    });

    it('fires from the debounced flush; a transient failure neither faults the activation nor loses the write', async () => {
        const seen: Seen[] = [];
        const wb = wbActor('WBFlush', 5, seen);
        const { storage, failing } = flakyStorage();
        const host = createHost({ actors: [wb], storage, defaults: quiet });
        const client = host.actor(wb, 'w');
        failing.on = true;
        await client.bump();
        await vi.waitFor(() => expect(seen).toHaveLength(1));
        expect(seen[0]).toEqual({ key: 'w', error: expect.any(Error), phase: 'flush', n: 1 });
        // Still serving: a transient storage error is not a state conflict.
        // The failed flush re-armed nothing — this WRITE is what schedules
        // the next one, and the dirty state rides along with it.
        await expect(client.bump()).resolves.toBe(2);
        // Storage recovers before that flush fires.
        failing.on = false;
        await vi.waitFor(async () => {
            const record = await storage.load('WBFlush', 'w');
            expect((record?.state as { n: number } | undefined)?.n).toBe(2);
        });
        await host.stop({ timeoutMs: 1000 });
        expect(seen).toHaveLength(1);
    });

    it('reports a debounced-flush etag conflict as a state-conflict error and discards the activation', async () => {
        const seen: Seen[] = [];
        const deactivated: DeactivationReason[] = [];
        const wb = wbActor('WBConflict', 5, seen, true, deactivated);
        const storage = memoryStorage();
        const host = createHost({ actors: [wb], storage, defaults: quiet });
        try {
            const client = host.actor(wb, 'w');
            await client.bump();
            await vi.waitFor(async () =>
                expect(await storage.load('WBConflict', 'w')).not.toBeNull()
            );
            // A second writer clobbers the record behind the activation's back.
            const record = await storage.load('WBConflict', 'w');
            await storage.save('WBConflict', 'w', { n: 99 }, record!.etag);
            await client.bump();
            await vi.waitFor(() => expect(seen).toHaveLength(1));
            expect(seen[0]!.phase).toBe('flush');
            expect(seen[0]!.error).toSatisfy(
                (e: unknown) => isActorError(e) && e.kind === 'state-conflict'
            );
            // The conflict discards the activation from the flush itself
            // (#336) — no further call is needed to make the runtime notice
            // its own fault. Before, the faulted activation sat `active`
            // until its next call (forever, if none came).
            await vi.waitFor(() => expect(deactivated).toEqual(['conflict']));
            // ...and the next call reloads the winning state, as documented.
            await expect(client.bump()).resolves.toBe(100);
            // A discarded activation flushes nothing: no 'final-flush' report.
            expect(seen).toHaveLength(1);
        } finally {
            await host.stop({ timeoutMs: 1000 });
        }
    });

    it('a throwing hook is swallowed (dev-logged) and deactivation still completes', async () => {
        const wb = defineActor({
            type: 'WBThrows',
            allowAnonymous: true,
            state: () => ({ n: 0 }),
            persistence: { mode: 'write-behind', debounceMs: 60_000 },
            onStateError() {
                throw new Error('pager is down too');
            },
            methods: (ctx) => ({
                async bump() {
                    ctx.state.n++;
                }
            })
        });
        const { storage, failing } = flakyStorage();
        const error = vi.spyOn(console, 'error').mockImplementation(() => {});
        try {
            const host = createHost({ actors: [wb], storage, defaults: quiet });
            await host.actor(wb, 'w').bump();
            failing.on = true;
            await expect(host.deactivateType('WBThrows')).resolves.toBeUndefined();
            expect(error.mock.calls.flat().join(' ')).toMatch(/onStateError of WBThrows\/w threw/);
        } finally {
            error.mockRestore();
        }
    });

    it('without a hook, a failed debounced flush is dev-logged instead of vanishing', async () => {
        const wb = wbActor('WBSilent', 5, [], false);
        const { storage, failing } = flakyStorage();
        const error = vi.spyOn(console, 'error').mockImplementation(() => {});
        try {
            const host = createHost({ actors: [wb], storage, defaults: quiet });
            failing.on = true;
            await host.actor(wb, 'w').bump();
            await vi.waitFor(() =>
                expect(error.mock.calls.flat().join(' ')).toMatch(
                    /write-behind flush of WBSilent\/w failed/
                )
            );
            failing.on = false;
            await host.stop({ timeoutMs: 1000 });
        } finally {
            error.mockRestore();
        }
    });
});
