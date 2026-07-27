/**
 * `useActorState()` — an actor read as a component's data source.
 *
 * Built ON `useData`, deliberately NOT on the `AsyncEngine` seam. Three
 * reasons, in order of weight:
 *
 *  1. `ASYNC_ENGINE_TOKEN` is app-exclusive, and `defaultAsyncEngine` exists
 *     precisely so a pack can delegate reads it has no policy for — that
 *     pack is `@sigx/cache`. If actors claimed the token,
 *     `app.use(cachePlugin()).use(actorsPlugin())` would be unresolvable,
 *     and actors would owe delegation semantics for every non-actor read on
 *     the page. Actors is a satellite; claiming an app-exclusive core seam
 *     is the wrong posture.
 *  2. An engine is for cross-cutting policy over reads you do NOT control.
 *     Actors authors 100% of its own call sites, so any policy applies here
 *     instead.
 *  3. `useData` already does the four hardest things: canonical tuple keys,
 *     the SSR `_useAsync` provider, `peekRestored`/`writeBack` hydration
 *     without refetch, and in-flight dedupe by canonical key — which is the
 *     entire answer to "ten components reading one actor = ten POSTs".
 *
 * SSR seeding therefore needs NO code here: on the server the fetcher's
 * `actor()` dispatches in-process through the silo seam, `useData` resolves
 * during the render, and core's state-serialization plugin writes the value
 * into the page under the same canonical key the client then reads.
 */
import { useData, type AsyncState } from 'sigx';
import { actor } from '../index';
import { actorKey, type ActorKeyArgs } from '../actor-key';
import type { ActorArgs, ActorReadName, ActorResult, AnyActorDefinition } from '../types';

/** Core's falsy-key vocabulary: a falsy key parks the read in `'idle'`. */
type Falsy = null | undefined | false | '';

/**
 * The SSR failure mode most likely to be misdiagnosed as a hook problem.
 *
 * A guard reading `rq.request` works over the wire and dies during a render
 * — but only when the SSR entry never imported `@sigx/server`, which is
 * what stamps the `__SIGX_SERVERFN_SCOPE__` seam the document handler opens
 * the request scope through. Without it `resolveServerContext` falls back
 * to a detached context whose `rq.request` getter throws, and the message
 * says only "nothing supplied a request" — true, but it points at the call
 * rather than at the entry that has to change. Dev builds add that.
 */
function explainMissingScope(error: unknown, def: AnyActorDefinition, method: string): unknown {
    // No environment test: this message can only come from an IN-PROCESS
    // dispatch, so matching it already means we are on the server. Checking
    // for `document` would be both redundant and wrong — a server render
    // under a DOM shim (happy-dom, jsdom) has one.
    if (!(error instanceof Error) || !/nothing\s+supplied a request/.test(error.message)) {
        return error;
    }
    error.message +=
        `\n[sigx actors] useActorState(${def.type}, …, '${method}') hit this DURING A ` +
        `SERVER RENDER: a guard on this actor read the request, but no request scope was ` +
        `open. The scope comes from @sigx/server — make sure your SSR entry imports it ` +
        `(a serverPlugin() install is enough), and that the render runs through the ` +
        `document handler rather than a bare renderToString.`;
    return error;
}

export interface UseActorStateOptions {
    /** Run the read during SSR. Default true — passed through to `useData`. */
    server?: boolean;
}

/** The reactive form's tuple: the actor key, the method, then its arguments. */
export type ActorCall<D, M extends ActorReadName<D>> = readonly [
    key: string,
    method: M,
    ...args: ActorKeyArgs<ActorArgs<D, M>>
];

/** Read an actor method as component data. */
export function useActorState<D extends AnyActorDefinition, M extends ActorReadName<D>>(
    def: D,
    key: string,
    method: M,
    ...args: ActorKeyArgs<ActorArgs<D, M>>
): AsyncState<ActorResult<D, M>>;
/**
 * Reactive form — mirrors `useData(() => [...])`. A falsy return parks the
 * read in `'idle'`, so `() => selectedId() && [selectedId(), 'total']` is
 * the idiom for "nothing selected yet".
 */
export function useActorState<D extends AnyActorDefinition, M extends ActorReadName<D>>(
    def: D,
    call: () => ActorCall<D, M> | Falsy,
    options?: UseActorStateOptions
): AsyncState<ActorResult<D, M>>;
export function useActorState(
    def: AnyActorDefinition,
    keyOrCall: string | (() => readonly unknown[] | Falsy),
    methodOrOptions?: string | UseActorStateOptions,
    ...rest: unknown[]
): AsyncState<unknown> {
    const reactive = typeof keyOrCall === 'function';
    const options = (reactive ? methodOrOptions : undefined) as
        | UseActorStateOptions
        | undefined;
    const call = reactive
        ? (keyOrCall as () => readonly unknown[] | Falsy)
        : () => [keyOrCall, methodOrOptions as string, ...rest] as const;

    return useData(
        () => {
            const next = call();
            if (!next) return null;
            const [key, method, ...args] = next as [string, string, ...unknown[]];
            // eslint-disable-next-line @typescript-eslint/no-explicit-any
            return actorKey(def, key, method as any, ...(args as any));
        },
        // Arguments are read back OUT OF THE KEY rather than closed over, so
        // a stale closure can never serve a fresher key's result. `actor()`
        // is isomorphic, so this one expression is the wire call in the
        // browser and an in-process dispatch during SSR.
        async (canonical) => {
            const [, , key, method, ...args] = canonical as [
                string,
                string,
                string,
                string,
                ...unknown[]
            ];
            const client = actor(def, key) as unknown as Record<
                string,
                (...a: unknown[]) => Promise<unknown>
            >;
            if (!__DEV__) return client[method]!(...args);
            try {
                return await client[method]!(...args);
            } catch (error) {
                throw explainMissingScope(error, def, method);
            }
        },
        options
    ) as AsyncState<unknown>;
}
