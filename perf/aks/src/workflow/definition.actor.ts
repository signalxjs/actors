/**
 * `WorkflowDefinition` — one actor per workflow NAME, holding every
 * version of its definition. Every run reads its definition here on its
 * first activation, which makes this the engine's hot shared key: with
 * N hosts and hash placement, (N−1)/N of all run starts pay a remote hop
 * to it. That is a locality axis the scenarios measure, not a flaw to
 * hide — it is how a definition store behaves in a real engine too.
 *
 * `get` never saves and never mutates state: the read counter lives in the
 * process-wide counters, so the hot key's record has exactly one writer
 * (`put`) and never CAS-conflicts with itself.
 */
import { defineActor } from '../actors.app.ts';
import { workflowCounters } from './counters.ts';
import type { WorkflowDef } from './types.ts';

export const WorkflowDefinition = defineActor({
    type: 'WorkflowDefinition',
    // Public on purpose: the load generator seeds definitions bare.
    allowAnonymous: true,
    state: () => ({ versions: {} as Record<string, WorkflowDef>, latest: 0 }),
    methods: (ctx) => ({
        /** Idempotent by version: a re-put of an existing version is a
         *  no-op, so every loadgen pod may seed. */
        async put(def: WorkflowDef): Promise<{ version: number; created: boolean }> {
            if (def.name !== ctx.key) {
                throw new Error(
                    `[workflow] definition '${def.name}' put under key '${ctx.key}'`
                );
            }
            const slot = String(def.version);
            if (ctx.state.versions[slot]) return { version: def.version, created: false };
            ctx.state.versions[slot] = def;
            if (def.version > ctx.state.latest) ctx.state.latest = def.version;
            await ctx.save();
            return { version: def.version, created: true };
        },
        async get(version?: number): Promise<{ version: number; def: WorkflowDef }> {
            workflowCounters.defReads++;
            const v = version ?? ctx.state.latest;
            const def = ctx.state.versions[String(v)];
            if (!def) throw new Error(`[workflow] unknown workflow ${ctx.key}@${v}`);
            // Detached: the caller gets a plain object, not a view onto
            // this activation's state proxy.
            return { version: v, def: ctx.snapshot(def) };
        },
        async versions(): Promise<number[]> {
            return Object.keys(ctx.state.versions)
                .map(Number)
                .sort((a, b) => a - b);
        }
    })
});
