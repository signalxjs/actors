/**
 * `useActorAction()` — an actor mutation as a component action.
 *
 * Built on core's `useAction`, so the mutation ergonomics every sigx app
 * already knows come free: `run(args)` that never rejects, `.loading` as the
 * blessed double-submit guard, `.match()` arms, and superseded semantics
 * when a second run starts before the first settles.
 *
 * On success it invalidates the reads it staled. The default is the
 * WHOLE-ACTOR prefix — `actorKey(def, key)` — rather than a guess at which
 * methods a write touched: `post()` changing what `recent()` returns is the
 * normal case, and a default that UNDER-invalidates leaves stale data on
 * screen, which is a worse failure than one redundant refetch.
 */
import { useAction, type AsyncAction } from '@sigx/runtime-core';
import { actor } from '../index';
import { actorKey } from '../actor-key';
import { useActorsContext } from './context';
import type { InvalidationPattern } from './cell-registry';
import type { ActorArgs, ActorReadName, ActorResult, AnyActorDefinition } from '../types';

export interface UseActorActionOptions {
    /**
     * What this write makes stale. Default: `[actorKey(def, key)]` — every
     * read of this actor. `false` disables invalidation entirely, for a
     * write no mounted read reflects.
     */
    invalidates?:
        | false
        | readonly InvalidationPattern[]
        | ((result: unknown, key: string) => readonly InvalidationPattern[]);
}

/**
 * `key` may be a getter so a component bound to a changing id does not have
 * to rebuild the action; it is read at `run()` time, never captured.
 */
export function useActorAction<D extends AnyActorDefinition, M extends ActorReadName<D>>(
    def: D,
    key: string | (() => string),
    method: M,
    options: UseActorActionOptions = {}
): AsyncAction<ActorResult<D, M>, ActorArgs<D, M>> {
    const registry = useActorsContext().cells;
    return useAction(async (args: ActorArgs<D, M>) => {
        const resolved = typeof key === 'function' ? key() : key;
        const client = actor(def, resolved) as unknown as Record<
            string,
            (...a: unknown[]) => Promise<unknown>
        >;
        const result = (await client[method]!(...(args ?? []))) as ActorResult<D, M>;

        // AFTER the await, so a failed write invalidates nothing: the
        // rejection surfaces through `useAction` and every read keeps the
        // value it has, which is still the truth.
        if (options.invalidates !== false) {
            const patterns =
                typeof options.invalidates === 'function'
                    ? options.invalidates(result, resolved)
                    : (options.invalidates ?? [actorKey(def, resolved)]);
            registry.invalidate(patterns);
        }
        return result;
    }) as AsyncAction<ActorResult<D, M>, ActorArgs<D, M>>;
}
