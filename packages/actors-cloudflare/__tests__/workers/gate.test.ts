/**
 * What `blockConcurrencyWhile` ACTUALLY does, measured rather than assumed.
 *
 * Both of these were asserted in #139/#140 from reasoning about the platform.
 * One of them was wrong, and the fake used there could not have told us:
 * modelling the gate as a non-reentrant queue produced a deadlock that the
 * real gate does not have. These tests pin the real behaviour so the next
 * person reasons from evidence.
 */
import { env, runInDurableObject } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import type { Env } from './fixture-worker';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

describe('the real blockConcurrencyWhile', () => {
    it('PERMITS re-entry — it does not deadlock', async () => {
        // #140 claimed the opposite, and restructured `onAlarm()` around it.
        // The restructure is still right (see below), but not for this
        // reason: a nested call resolves normally.
        const stub = env.ACTORS.get(env.ACTORS.idFromName('gate-reentry'));
        await runInDurableObject(stub, async (_instance, state) => {
            const outcome = await Promise.race([
                state.blockConcurrencyWhile(async () => {
                    await state.blockConcurrencyWhile(async () => 'inner');
                    return 'nested-ok';
                }),
                new Promise<string>((resolve) => setTimeout(() => resolve('HUNG'), 1_500))
            ]);
            expect(outcome).toBe('nested-ok');
        });
    });

    it('BREAKS the object when its callback throws', async () => {
        // This half of #140 was right, and it is why an expected, diagnosable
        // failure (`this Durable Object hosts a different actor`) is returned
        // as a value and raised only once the gate has closed.
        const stub = env.ACTORS.get(env.ACTORS.idFromName('gate-throw'));
        await runInDurableObject(stub, async (_instance, state) => {
            await state.storage.put('marker', 'before');
        });

        await expect(
            runInDurableObject(stub, async (_instance, state) => {
                await state.blockConcurrencyWhile(async () => {
                    throw new Error('boom-inside-gate');
                });
            })
        ).rejects.toThrow(/boom-inside-gate/);

        // The object itself is now unusable — every later call on it fails
        // with the original error rather than doing anything.
        await expect(
            runInDurableObject(stub, async (_instance, state) => {
                await state.storage.get('marker');
            })
        ).rejects.toThrow(/boom-inside-gate/);
    });
});
