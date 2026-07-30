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
    // OWN keys only: `methodUse?.[method]` resolved `Object.prototype`
    // members, and since a function's `.length` is its arity the
    // empty-chain early return below did not fire — the loop then iterated
    // a FUNCTION, so a prototype name 500'd here before dispatch could
    // answer its honest 404.
    const methodChain =
        opts.methodUse && Object.hasOwn(opts.methodUse, method)
            ? opts.methodUse[method]
            : undefined;
    if (!actorChain?.length && !methodChain?.length) return;
    const info = guardInfo(def, method);
    if (actorChain) for (const guard of actorChain) await guard(rq, info);
    if (methodChain) for (const guard of methodChain) await guard(rq, info);
}
