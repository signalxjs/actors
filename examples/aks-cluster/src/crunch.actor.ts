// The CPU-bound workload: a sha256 hash-chain burns a controllable amount
// of processor per call. This is what pushes silo CPU past the HPA
// threshold in the autoscaling scenario — the Counter's Redis round-trips
// barely warm a core.
import { createHash } from 'node:crypto';
import { defineActor } from './actors.app.ts';

export const Crunch = defineActor({
    type: 'Crunch',
    unguarded: true,
    state: () => ({ burns: 0, last: '' }),
    methods: (ctx) => ({
        async burn(iterations: number, payloadBytes: number) {
            let digest = Buffer.alloc(Math.max(1, payloadBytes), 7);
            for (let i = 0; i < iterations; i++) {
                digest = createHash('sha256').update(digest).digest();
            }
            ctx.state.burns += 1;
            ctx.state.last = digest.toString('hex');
            await ctx.save();
            return ctx.state.last;
        },
        async stats() {
            return { burns: ctx.state.burns };
        }
    })
});
