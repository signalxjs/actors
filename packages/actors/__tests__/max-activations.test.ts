/**
 * defaults.maxActivations (#16) — the LRU capacity shed.
 *
 * The contract: a SOFT cap enforced by the sweeper. Excess activations are
 * shed least-recently-used first with reason 'capacity'; busy or held
 * activations are never shed (the count may sit over the cap while the
 * host is genuinely loaded); a shed actor re-activates on its next call
 * with state intact; 0 (the default) disables the whole pass.
 */
import { beforeEach, describe, expect, it } from 'vitest';
import { defineActor, type DeactivationReason, type Host } from '@sigx/actors';
import { createHost, manualScheduler, memoryStorage } from '@sigx/actors/host';

const reasons: Record<string, DeactivationReason[]> = {};
const holds = new Map<string, () => void>();

beforeEach(() => {
    // Module-scoped recorders: host.stop() in each finally appends
    // 'shutdown' entries, so every test starts from a clean slate.
    for (const key of Object.keys(reasons)) delete reasons[key];
    holds.clear();
});

const Counter = defineActor({
    type: 'Counter',
    unguarded: true,
    state: () => ({ count: 0 }),
    methods: (ctx) => ({
        async bump() {
            ctx.state.count += 1;
            await ctx.save();
            return ctx.state.count;
        },
        /** Parks the activation until the test releases it — a busy actor. */
        async hold() {
            await new Promise<void>((release) => holds.set(ctx.key, release));
        }
    }),
    onDeactivate: (ctx, reason) => {
        (reasons[ctx.key] ??= []).push(reason);
    }
});

function build(maxActivations?: number) {
    const scheduler = manualScheduler();
    const host = createHost({
        actors: [Counter],
        storage: memoryStorage(),
        scheduler,
        defaults: {
            // Idle collection effectively off: only the capacity pass acts.
            idleAfterMs: 3_600_000,
            sweepIntervalMs: 1_000,
            callTimeoutMs: 0,
            ...(maxActivations !== undefined ? { maxActivations } : {})
        }
    });
    return { scheduler, host };
}

/** Let deactivation drains settle. */
async function drain(): Promise<void> {
    for (let i = 0; i < 10; i++) await new Promise((r) => setTimeout(r, 2));
}

async function stop(host: Host): Promise<void> {
    for (const release of holds.values()) release();
    holds.clear();
    await host.stop({ timeoutMs: 1000 });
}

describe('defaults.maxActivations', () => {
    it('sheds the least-recently-used excess, reason capacity', async () => {
        const { scheduler, host } = build(3);
        await host.start();
        try {
            // Six activations, touched in order — a1 oldest, a6 newest.
            for (let i = 1; i <= 6; i++) {
                await host.actor(Counter, `a${i}`).bump();
                await new Promise((r) => setTimeout(r, 5)); // distinct lastActivityMs
            }
            expect(host.stats().activations).toBe(6);
            scheduler.advance(1_000);
            await drain();
            expect(host.stats().activations).toBe(3);
            // The LRU order: the three OLDEST were shed, each as 'capacity'.
            for (const key of ['a1', 'a2', 'a3']) {
                expect(reasons[key]).toEqual(['capacity']);
            }
            const survivors = host
                .activations()
                .map((a) => a.key)
                .sort();
            expect(survivors).toEqual(['a4', 'a5', 'a6']);
        } finally {
            await stop(host);
        }
    });

    it('a shed actor re-activates on its next call with state intact', async () => {
        const { scheduler, host } = build(1);
        await host.start();
        try {
            await host.actor(Counter, 'keep').bump();
            await new Promise((r) => setTimeout(r, 5));
            await host.actor(Counter, 'shed-me').bump();
            scheduler.advance(1_000);
            await drain();
            expect(host.stats().activations).toBe(1);
            // 'keep' was older → shed; calling it again reloads its state.
            await expect(host.actor(Counter, 'keep').bump()).resolves.toBe(2);
        } finally {
            await stop(host);
        }
    });

    it('never sheds a busy activation — the cap is soft', async () => {
        const { scheduler, host } = build(1);
        await host.start();
        try {
            // Two actors parked mid-turn, one quiet. Only the quiet one is
            // a candidate, so the host stays OVER the cap.
            const parked1 = host.actor(Counter, 'busy1').hold();
            const parked2 = host.actor(Counter, 'busy2').hold();
            await new Promise((r) => setTimeout(r, 10));
            await host.actor(Counter, 'quiet').bump();
            expect(host.stats().activations).toBe(3);
            scheduler.advance(1_000);
            await drain();
            expect(host.stats().activations).toBe(2);
            expect(reasons['quiet']).toEqual(['capacity']);
            expect(reasons['busy1']).toBeUndefined();
            expect(reasons['busy2']).toBeUndefined();
            for (const release of holds.values()) release();
            holds.clear();
            await Promise.all([parked1, parked2]);
        } finally {
            await stop(host);
        }
    });

    it('0 or omitted disables the pass entirely', async () => {
        const { scheduler, host } = build();
        await host.start();
        try {
            for (let i = 0; i < 5; i++) await host.actor(Counter, `n${i}`).bump();
            scheduler.advance(1_000);
            await drain();
            expect(host.stats().activations).toBe(5);
        } finally {
            await stop(host);
        }
    });
});
