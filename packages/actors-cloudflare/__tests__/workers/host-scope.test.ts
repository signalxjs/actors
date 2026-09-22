/**
 * Two objects of one class in one isolate, and an AMBIENT hop between them
 * (#456) — on real workerd, because only workerd enforces that an object
 * performs I/O for itself alone.
 *
 * The ambient seam is one global, last-wins, and every object stamps it on
 * boot. Without the per-request host scope, `actor()` inside object A
 * resolved through object B's host once B had booted after A; B's placement
 * answers `isSelf` for B's actor, so B's actor ran INSIDE A, on B's storage:
 * "Cannot perform I/O on behalf of a different Durable Object", and B reset.
 */
import { SELF } from 'cloudflare:test';
import { describe, expect, it } from 'vitest';
import { encodeSymbolPath } from '../../../actors/src/wire-url';
import type { Env } from './fixture-worker';

declare module 'cloudflare:test' {
    interface ProvidedEnv extends Env {}
}

async function invoke(symbol: string, args: readonly unknown[]): Promise<unknown> {
    const res = await SELF.fetch(`https://edge.test/_sigx/actor/${encodeSymbolPath(symbol)}`, {
        method: 'POST',
        headers: { 'content-type': 'application/json' },
        body: JSON.stringify({ args })
    });
    const body = (await res.json()) as { data?: unknown; error?: { message?: string } };
    if (!res.ok || body.error) {
        throw new Error(body.error?.message ?? `HTTP ${res.status}`);
    }
    return body.data;
}

describe('host scope on workerd (#456)', () => {
    it('sends an ambient hop to the callee object when the callee booted last', async () => {
        // A boots, then B — B's host is the one the global names.
        await invoke('Counter#increment', ['scope-a', 1]);
        await invoke('Counter#increment', ['scope-b', 1]);

        await expect(invoke('Counter#bumpPeerAmbient', ['scope-a', 'scope-b'])).resolves.toBe(2);
        // B was not reset: its count survived, and the hop's save landed.
        await expect(invoke('Counter#read', ['scope-b'])).resolves.toBe(2);
        await expect(invoke('Counter#read', ['scope-a'])).resolves.toBe(1);
    });

    it('keeps each caller on its own host across concurrent ambient hops', async () => {
        // Fan-in, not a cycle: a cycle of ambient hops is a real deadlock
        // (no call chain rides an ambient hop, so reentrancy cannot see it).
        for (const key of ['scope-x', 'scope-y', 'scope-z', 'scope-w']) {
            await invoke('Counter#increment', [key, 1]);
        }
        const hops = await Promise.all(
            ['scope-x', 'scope-y', 'scope-z'].map((key) =>
                invoke('Counter#bumpPeerAmbient', [key, 'scope-w'])
            )
        );
        expect([...(hops as number[])].sort()).toEqual([2, 3, 4]);
        await expect(invoke('Counter#read', ['scope-w'])).resolves.toBe(4);
    });
});
