/**
 * Turns an actor starts on its OWN clock — a `ctx.timer` tick, a task's
 * `ctx.turn` — carry the host's default deadline (#302). Before this, the
 * context they minted had no `deadline`, `ctx.actor()` relayed that absence
 * across the hop, and a cross-host call from such a turn could wait forever:
 * the wedge that pinned a whole fetch pool on the workflow rig.
 */
import { afterEach, describe, expect, it, vi } from 'vitest';
import { defineActor, isActorError } from '@sigx/actors';
import { createCluster, selfPolicy, type ClusterHarness } from './harness';

let running: ClusterHarness | null = null;
afterEach(async () => {
    await running?.stop();
    running = null;
});

/** A peer that never answers: `hang()` parks its turn for good. */
function stuckActor() {
    return defineActor({
        type: 'Stuck',
        allowAnonymous: true,
        state: () => ({}),
        methods: () => ({
            async warm() {
                return 'ok';
            },
            async hang() {
                await new Promise(() => {});
            }
        })
    });
}

function outcomeOf(promise: Promise<unknown>, outcomes: unknown[]): Promise<void> {
    return promise.then(
        () => void outcomes.push('resolved'),
        (error: unknown) => void outcomes.push(error)
    );
}

function expectCallTimeout(outcomes: unknown[]): void {
    expect(outcomes).toHaveLength(1);
    const error = outcomes[0];
    // Over a cluster hop the peer's ActorCallTimeoutError rehydrates as an
    // ActorError carrying its `kind`, so assert on the kind and let a wrong
    // outcome (a 'resolved' string, another kind) print itself on failure.
    expect(isActorError(error)).toBe(true);
    expect(error).toMatchObject({ kind: 'call-timeout' });
}

describe('deadlines on self-started turns (#302)', () => {
    it('a cross-host call from a timer tick rejects at callTimeoutMs instead of hanging', async () => {
        const stuck = stuckActor();
        const outcomes: unknown[] = [];
        const poller = defineActor({
            type: 'Poller',
            allowAnonymous: true,
            state: () => ({}),
            onActivate(ctx) {
                ctx.timer('poll', () => outcomeOf(ctx.actor(stuck, 's').hang(), outcomes), {
                    due: 1
                });
            },
            methods: () => ({
                async warm() {
                    return 'ok';
                }
            })
        });
        const cluster = await createCluster(2, {
            actors: [stuck, poller],
            policy: selfPolicy,
            defaults: { callTimeoutMs: 60 }
        });
        running = cluster;
        // Stuck/s lives on host 1; Poller/p activates on host 0, so the tick's
        // call is a real hop.
        await cluster.hosts[1]!.actor(stuck, 's').warm();
        await cluster.hosts[0]!.actor(poller, 'p').warm();

        await vi.waitFor(() => expectCallTimeout(outcomes), { timeout: 1000 });
    });

    it("a cross-host call from a task's ctx.turn rejects at callTimeoutMs instead of hanging", async () => {
        const stuck = stuckActor();
        const outcomes: unknown[] = [];
        const runner = defineActor({
            type: 'Runner',
            allowAnonymous: true,
            state: () => ({}),
            methods: (ctx) => ({
                begin: () => ctx.tasks.start('run')
            }),
            tasks: (ctx) => ({
                async run() {
                    await outcomeOf(
                        ctx.turn((c) => c.actor(stuck, 's').hang()),
                        outcomes
                    );
                }
            })
        });
        const cluster = await createCluster(2, {
            actors: [stuck, runner],
            policy: selfPolicy,
            defaults: { callTimeoutMs: 60 }
        });
        running = cluster;
        await cluster.hosts[1]!.actor(stuck, 's').warm();
        await cluster.hosts[0]!.actor(runner, 'r').begin();

        await vi.waitFor(() => expectCallTimeout(outcomes), { timeout: 1000 });
    });
});
