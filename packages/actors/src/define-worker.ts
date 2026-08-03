/**
 * `defineWorker` — stateless multi-activation pure-compute actors.
 *
 * A worker type's activations are interchangeable: the runtime keeps a pool
 * of up to `maxLocal` members per (type, key) on each host, dispatches to
 * the fewest queued turns, never claims the distributed directory, and always
 * places locally. Two calls to the same key may run concurrently on
 * different members — that is the contract, and it is why everything
 * identity-bound is structurally absent from {@link WorkerOptions}.
 *
 * Like `defineJob`, this is convention over `defineActor`: one call with a
 * hidden `stateless` marker, returning a plain `ActorDefinition` so
 * `actor()`, the client proxy and the app hooks all work unchanged.
 */
import { defineActor } from './define';
import type {
    ActorDefinition,
    ActorMethodTable,
    ActorOptions,
    ActorStreamTable,
    WorkerOptions
} from './types';

export function defineWorker<
    M extends ActorMethodTable,
    St extends ActorStreamTable = Record<never, never>
>(options: WorkerOptions<M, St>): ActorDefinition<Record<never, never>, M, St> {
    // A resource commitment, so it throws in EVERY build (the `define.ts`
    // doctrine): a fractional or non-positive cap would surface as a pool
    // that silently never grows — or never serves — on whichever host
    // activates the type first.
    if (
        options.maxLocal !== undefined &&
        (!Number.isInteger(options.maxLocal) || options.maxLocal < 1)
    ) {
        throw new Error(
            `[sigx actors] worker "${options.type}" needs a positive integer \`maxLocal\`.`
        );
    }
    type S = Record<never, never>;
    const pass: Partial<ActorOptions<S, M, St>> = {
        ...(options.use ? { use: options.use } : {}),
        ...(options.unguarded !== undefined ? { unguarded: options.unguarded } : {}),
        ...(options.methodUse ? { methodUse: options.methodUse } : {}),
        ...(options.idleAfterMs !== undefined ? { idleAfterMs: options.idleAfterMs } : {}),
        ...(options.onActivate ? { onActivate: options.onActivate } : {}),
        ...(options.onDeactivate ? { onDeactivate: options.onDeactivate } : {}),
        ...(options.reads ? { reads: options.reads } : {}),
        ...(options.streams ? { streams: options.streams } : {})
    };
    return defineActor<S, M, St>({
        ...pass,
        type: options.type,
        // Never loaded, never saved — the activation skips storage entirely
        // for stateless types; this factory only satisfies the option shape.
        state: () => ({}),
        methods: options.methods,
        stateless: options.maxLocal !== undefined ? { maxLocal: options.maxLocal } : {}
    });
}
