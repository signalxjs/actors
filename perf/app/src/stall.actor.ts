/**
 * A deliberately blocking turn — the diagnostic for one specific question:
 * what happens to a host whose event loop stops?
 *
 * It matters because the membership heartbeat is a `setInterval` on the
 * SAME loop that runs actor turns. With `redisCluster`'s defaults —
 * `heartbeatMs: 5000`, `ttlMs: 15000` — a turn that computes synchronously
 * for longer than the TTL starves its own liveness signal. If that plays
 * out the way the code reads, a busy host is indistinguishable from a dead
 * one: peers drop it, it self-fences, and self-fencing is fatal by design
 * (#141), so the kubelet restarts it. One CPU-bound actor would then be
 * able to kill an otherwise healthy host and take every other actor on it
 * along.
 *
 * That is a claim about behaviour, and claims about behaviour get measured
 * rather than asserted — hence this.
 *
 * OFF unless `ENABLE_STALL=1`. It is a denial-of-service primitive on a
 * public endpoint: a guard stops anonymous callers, not a signed-in one
 * from wedging a host. Capped as well, so a typo cannot park a host for an
 * hour.
 */
import { ServerFnError } from '@sigx/server';
import { randomUUID } from 'node:crypto';
import { defineActor } from './actors.app';

const MAX_MS = Number(process.env.STALL_MAX_MS ?? 30_000);

export const Stall = defineActor({
    type: 'Stall',
    state: () => ({ stalls: 0 }),
    methods: (ctx) => {
        const member = randomUUID().slice(0, 8);
        return {
            /**
             * Spin — not sleep. `setTimeout` would yield the loop and prove
             * nothing; the whole point is to hold the thread so timers,
             * probes and the heartbeat cannot run.
             */
            async block(ms: number): Promise<{ member: string; blockedMs: number }> {
                if (process.env.ENABLE_STALL !== '1') {
                    throw new ServerFnError(403, 'the stall diagnostic is disabled');
                }
                const budget = Math.min(MAX_MS, Math.max(0, Math.trunc(ms)));
                const startedAt = Date.now();
                while (Date.now() - startedAt < budget) {
                    // deliberately empty
                }
                ctx.state.stalls += 1;
                return { member, blockedMs: Date.now() - startedAt };
            }
        };
    }
});
