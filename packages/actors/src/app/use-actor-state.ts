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
import { getCurrentInstance, onMounted, onUnmounted, useData, type AsyncState } from '@sigx/runtime-core';
import {
    makeUnhandledReporter,
    matchAsyncState,
    registerHandledAsyncOptionKeys,
    writeBack
} from '@sigx/runtime-core/internals';
import { effect, signal, untrack } from '@sigx/reactivity';
import { actor } from '../index';
import { actorKey, type ActorKeyArg, type ActorKeyArgs } from '../actor-key';
import { useActorsContext } from './context';
import type { LiveChannel } from './live';
import type { ActorArgs, ActorReadName, ActorResult, AnyActorDefinition } from '../types';

/**
 * `live` is ours, not core's. Without this the default engine's
 * unknown-option warning fires on every live read — that warning exists to
 * catch a missing plugin install, and a key the caller's plugin DOES handle
 * would make it cry wolf.
 */
registerHandledAsyncOptionKeys('live');

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
    /**
     * Subscribe to server pushes for this read: the actor re-runs the method
     * after every turn that mutated its state and the new result arrives on
     * the page's single live connection.
     *
     * What it adds is *other people's* writes. Everything else in the read
     * path only ever refreshes the tab that wrote — `useActorAction`'s
     * invalidation is local bookkeeping, and no request/response call tells a
     * second browser that anything happened.
     *
     * The first value still comes from the ordinary read (SSR-seeded and
     * hydrated without a refetch, deduped by canonical key); this is purely
     * additive on top of it. Client-only: a server render has nothing to push
     * to, so it is inert there.
     */
    live?: boolean;
}

/** The reactive form's tuple: the actor key, the method, then its arguments. */
export type ActorCall<D, M extends ActorReadName<D>> = readonly [
    key: string,
    method: M,
    ...args: ActorKeyArgs<ActorArgs<D, M>>
];

/**
 * Read an actor method as component data.
 *
 * Options come LAST, after the method's arguments — the only place a variadic
 * signature leaves for them. Unambiguous because a key argument must be a JSON
 * primitive (see `ActorKeyArgs`), so an object in that position can only be
 * the option bag.
 */
export function useActorState<D extends AnyActorDefinition, M extends ActorReadName<D>>(
    def: D,
    key: string,
    method: M,
    ...args: [...ActorKeyArgs<ActorArgs<D, M>>, options?: UseActorStateOptions]
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
    // Positional form: a non-primitive in the trailing position can only be
    // the option bag — key arguments are JSON primitives by contract, which is
    // what makes this unambiguous rather than a guess. Popped before the
    // closure below captures `rest`, so it never reaches the actor.
    const trailing =
        !reactive && isOptionBag(rest[rest.length - 1])
            ? (rest.pop() as UseActorStateOptions)
            : undefined;
    const options = (reactive ? methodOrOptions : trailing) as
        | UseActorStateOptions
        | undefined;
    const call = reactive
        ? (keyOrCall as () => readonly unknown[] | Falsy)
        : () => [keyOrCall, methodOrOptions as string, ...rest] as const;

    const keyOf = (): readonly ActorKeyArg[] | null => {
        const next = call();
        if (!next) return null;
        const [key, method, ...args] = next as [string, string, ...unknown[]];
        // eslint-disable-next-line @typescript-eslint/no-explicit-any
        return actorKey(def, key, method as any, ...(args as any));
    };

    const state = useData(
        keyOf,
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

    // Register with the app's cell registry so a write to this actor
    // refreshes this read. The key is passed as a GETTER, evaluated at
    // invalidation time: a reactive key registered by value would stop
    // matching the moment it changed, which is exactly the case (a list
    // whose selection moves) where a stale row is most visible.
    //
    // Outside a component there is no unmount to unregister on, so skip —
    // `useActorState` is a component hook, but `getCurrentInstance()` is
    // what makes that a graceful no-op rather than a leak.
    const instance = getCurrentInstance();
    if (!instance) return state;

    const context = useActorsContext();
    let release: (() => void) | null = null;
    onMounted(() => {
        release = context.cells.add(
            () => {
                const key = keyOf();
                return key ? JSON.stringify(key) : null;
            },
            () => void state.refresh()
        );
    });
    onUnmounted(() => {
        release?.();
        release = null;
    });

    return options?.live ? liveView(state, keyOf, context.live, instance) : state;
}

const OPTION_KEYS = new Set<string>(['server', 'live']);

/**
 * A trailing argument that can only be the option bag — see the overload.
 *
 * Narrow on purpose. "Any object" would be enough for typed callers (a key
 * argument must be a JSON primitive), but it would make a JavaScript caller's
 * mistake SILENT: `useActorState(D, 'k', 'at', new Date())` would swallow the
 * date as options instead of reaching `actorKey`'s dev-time argument check,
 * and the read would quietly address a different key than it looks like it
 * does. So: a plain object (no `Date`, `Map`, or class instance — their
 * prototypes fail the test) carrying only known option keys.
 *
 * An EMPTY plain object counts as options, which matters in the one direction
 * types cannot help with: `{}` type-checks as `UseActorStateOptions`, so it
 * must not throw. As an actor argument it is a type error already.
 */
function isOptionBag(value: unknown): boolean {
    if (typeof value !== 'object' || value === null) return false;
    const proto = Object.getPrototypeOf(value) as unknown;
    if (proto !== Object.prototype && proto !== null) return false;
    return Object.keys(value).every((key) => OPTION_KEYS.has(key));
}

/**
 * The pushed value, in front of the cell.
 *
 * A WRAPPER rather than a write into the cell, because core does not let one
 * happen: `AsyncState` is written only by the engine that owns it, and the
 * obvious workaround — `writeBack(key, value)` then `refresh()` — does not
 * work, since `refresh()` invalidates the restored entry before fetching and
 * would pay a round trip per pushed frame. (The upstream ask that deletes this
 * function is `AsyncReadHandle.setValue`.)
 *
 * Two rules make the composition honest:
 *
 *  - **A cell settle wins.** Any time the underlying read settles, the pushed
 *    value is dropped. It has to be: `useActorAction` invalidates on write, so
 *    the cell refetches and holds the post-write truth, while the push for
 *    that same mutation is still throttled (50 ms by default) somewhere behind
 *    it. Preferring the push there would show the writer their OLD value for
 *    exactly as long as the throttle lasts — the one case the invalidation
 *    story exists to get right.
 *  - **A dead feed degrades to "not live", not "broken".** A subscription
 *    error surfaces only when the cell has nothing of its own to show; with a
 *    value in hand the page keeps rendering it, because a silo whose placement
 *    cannot watch (501) must not take a working read down with it.
 */
function liveView(
    state: AsyncState<unknown>,
    keyOf: () => readonly ActorKeyArg[] | null,
    channel: LiveChannel,
    instance: NonNullable<ReturnType<typeof getCurrentInstance>>
): AsyncState<unknown> {
    // One reactive cell for both, so a push and a failure notify readers the
    // same way. `seq` is what makes an identical value re-notify.
    const pushed = signal({ has: false, value: undefined as unknown, error: null as Error | null });
    const report = makeUnhandledReporter(instance, 'useActorState');

    let stop: (() => void) | null = null;
    const subscribeTo = (tuple: readonly ActorKeyArg[] | null): void => {
        stop?.();
        stop = null;
        if (!tuple) return; // a falsy key: nothing to watch yet
        const [, type, key, method, ...args] = tuple as [
            string,
            string,
            string,
            string,
            ...ActorKeyArg[]
        ];
        const canonical = JSON.stringify(tuple);
        stop = channel.subscribe(
            { type, key, method, args },
            (value) => {
                pushed.error = null;
                pushed.value = value;
                pushed.has = true;
                // The page cache too, under the same canonical key the cell
                // reads: a component that unmounts and remounts restores from
                // the blob WITHOUT refetching, and the newest thing we know is
                // this push — not whatever the last actual fetch returned.
                writeBack(canonical, value);
            },
            (error) => {
                if (__DEV__) {
                    console.warn(
                        `[sigx actors] live subscription for ${type}/${key}#${method} failed; ` +
                            `this read still works, it just will not update by itself:`,
                        error
                    );
                }
                pushed.has = false;
                pushed.error = error;
            }
        );
    };

    onMounted(() => {
        let subscribed: string | null = null;
        const runner = effect(() => {
            const tuple = keyOf();
            const canonical = tuple ? JSON.stringify(tuple) : null;
            // Tracked on purpose: a settle of the cell's own is newer
            // information than anything pushed before it (see above).
            const settled = state.state === 'ready' || state.state === 'errored';
            void state.value;
            untrack(() => {
                if (canonical !== subscribed) {
                    subscribed = canonical;
                    pushed.has = false;
                    pushed.error = null;
                    subscribeTo(tuple);
                } else if (settled) {
                    pushed.has = false;
                }
            });
        });
        onUnmounted(() => {
            runner.stop();
            stop?.();
            stop = null;
        });
    });

    /**
     * Has the underlying read produced nothing at all yet?
     *
     * Tested through `state.state`, NOT `state.value === null`: `null` is a
     * value an actor read may legitimately return, and treating it as "nothing
     * to show" would let a failed subscription blank out a read that is
     * perfectly ready.
     */
    const unsettled = (): boolean => state.state === 'pending' || state.state === 'idle';

    const view: AsyncState<unknown> = {
        get state() {
            if (pushed.has) return 'ready';
            if (pushed.error && unsettled()) return 'errored';
            return state.state;
        },
        get value() {
            return pushed.has ? pushed.value : state.value;
        },
        get error() {
            if (pushed.has) return null;
            return state.error ?? (unsettled() ? pushed.error : null);
        },
        get loading() {
            return pushed.has ? false : state.loading;
        },
        match(arms) {
            return matchAsyncState(
                {
                    state: view.state,
                    value: view.value,
                    error: view.error,
                    // The last-good value for the error arm. A pushed value is
                    // as good as a fetched one.
                    stale: pushed.has ? pushed.value : state.value,
                    retry: () => void view.refresh(),
                    onUnhandledError: report
                },
                arms
            );
        },
        refresh() {
            // The caller asked for a fresh READ, so the push stops standing in
            // for one: whatever comes back is newer than what we are holding.
            pushed.has = false;
            pushed.error = null;
            return state.refresh();
        }
    };
    return view;
}
