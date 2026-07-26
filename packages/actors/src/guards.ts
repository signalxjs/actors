/**
 * The guard pipeline — shared by the wire resolver (`./server`) and the
 * in-process `actor()` entry so both transports run the SAME chains
 * (rfc-server-v3's lesson: definition-level chains are the only
 * transport-complete mechanism; endpoint guards are wire-only backstops).
 *
 * Guards run OUTSIDE the mailbox: a slow auth check never occupies the
 * actor's turn. Actor-to-actor calls (`ctx.actor`) are intra-system and do
 * NOT re-run guards — the system boundary already did (Orleans posture:
 * grain-to-grain calls are trusted).
 */
import type { ServerFnContext, ServerFnInfo } from '@sigx/server';
import type { AnyActorDefinition } from './types';

export function guardInfo(def: AnyActorDefinition, method: string): ServerFnInfo {
    return { symbol: `${def.type}#${method}`, name: method };
}

export async function runGuards(
    def: AnyActorDefinition,
    method: string,
    rq: ServerFnContext
): Promise<void> {
    const opts = def.__sigxActor;
    const actorChain = opts.use;
    const methodChain = opts.methodUse?.[method];
    if (!actorChain?.length && !methodChain?.length) return;
    const info = guardInfo(def, method);
    if (actorChain) for (const guard of actorChain) await guard(rq, info);
    if (methodChain) for (const guard of methodChain) await guard(rq, info);
}
