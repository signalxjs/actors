/**
 * `useActorAction()` — an actor mutation as a component action.
 *
 * Built on core's `useAction`, so the mutation ergonomics every sigx app
 * already knows come free: `run(args)` that never rejects, `.loading` as the
 * blessed double-submit guard, `.match()` arms, and superseded semantics
 * when a second run starts before the first settles.
 *
 * Note what this does NOT do yet: invalidate the reads a write makes stale.
 * That needs a registry of mounted cells, which arrives with the
 * invalidation step — `actorKey(def, key)` is already the prefix it will
 * use.
 */
import { useAction, type AsyncAction } from 'sigx';
import { actor } from '../index';
import type { ActorArgs, ActorReadName, ActorResult, AnyActorDefinition } from '../types';

/**
 * `key` may be a getter so a component bound to a changing id does not have
 * to rebuild the action; it is read at `run()` time, never captured.
 */
export function useActorAction<D extends AnyActorDefinition, M extends ActorReadName<D>>(
    def: D,
    key: string | (() => string),
    method: M
): AsyncAction<ActorResult<D, M>, ActorArgs<D, M>> {
    return useAction(async (args: ActorArgs<D, M>) => {
        const resolved = typeof key === 'function' ? key() : key;
        const client = actor(def, resolved) as unknown as Record<
            string,
            (...a: unknown[]) => Promise<unknown>
        >;
        return (await client[method]!(...(args ?? []))) as ActorResult<D, M>;
    }) as AsyncAction<ActorResult<D, M>, ActorArgs<D, M>>;
}
