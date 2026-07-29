/**
 * `mintCallId` — the cluster-wide correlation id.
 *
 * The interesting property is not the format, it is WHEN the per-process
 * random component is drawn. workerd refuses to generate random values in
 * global scope, so a module-scope seed is either rejected outright or served
 * from a deterministic startup seed — and in the latter case every isolate
 * gets the SAME seed while its counter restarts at zero, so two Durable
 * Objects mint colliding ids. That defeats deadlock detection and call-chain
 * correlation, which are the only reasons this id exists.
 */
import { describe, expect, it, vi } from 'vitest';

describe('mintCallId', () => {
    it('draws no randomness at module scope', async () => {
        // The guard for the workerd rule. Same class of hazard as the lazy
        // AbortController in `context.ts` — and the same fix.
        vi.resetModules();
        const random = vi.spyOn(Math, 'random');
        try {
            await import('../src/call-id');
            expect(random).not.toHaveBeenCalled();
        } finally {
            random.mockRestore();
        }
    });

    it('seeds on first use, then reuses that seed', async () => {
        vi.resetModules();
        const random = vi.spyOn(Math, 'random');
        try {
            const { mintCallId } = await import('../src/call-id');
            expect(random).not.toHaveBeenCalled();

            const first = mintCallId();
            expect(random).toHaveBeenCalledTimes(1);

            // Later mints reuse the seed: one draw per process, not per call.
            const second = mintCallId();
            expect(random).toHaveBeenCalledTimes(1);

            expect(first).not.toBe(second);
            // `c<counter>.<proc>.<time>` — the proc segment is what identifies
            // the host, so it must be stable across mints.
            expect(first.split('.')[1]).toBe(second.split('.')[1]);
        } finally {
            random.mockRestore();
        }
    });

    it('gives two isolates of the same bundle different seeds', async () => {
        // The Durable Object case: one bundle, many isolates, each starting
        // its counter at zero. If the seed were shared, `c1.<seed>.<time>`
        // would collide between objects minted in the same millisecond.
        //
        // Both draws are stubbed rather than left to chance: two real draws
        // agreeing is astronomically unlikely but not impossible, and a test
        // that fails once a year is worse than no test. Stubbing also lets
        // this assert the sharper property — exactly ONE draw per isolate.
        vi.resetModules();
        const random = vi
            .spyOn(Math, 'random')
            .mockReturnValueOnce(0.1)
            .mockReturnValueOnce(0.5);
        try {
            const a = await import('../src/call-id');
            vi.resetModules();
            const b = await import('../src/call-id');
            expect(a.mintCallId).not.toBe(b.mintCallId); // genuinely re-evaluated

            const first = a.mintCallId();
            const second = b.mintCallId();
            expect(random).toHaveBeenCalledTimes(2);
            expect(first.split('.')[1]).not.toBe(second.split('.')[1]);
        } finally {
            random.mockRestore();
        }
    });
});
