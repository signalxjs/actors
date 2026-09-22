/**
 * `runWithHost` — enter a host scope, so an ambient `actor()` / `publishTopic()`
 * inside `fn` resolves through THIS host rather than the last one started
 * (#456).
 *
 * One host per process needs none of this: the stamped global is right.
 * Several in one process — every Durable Object isolate, one host per
 * object — makes last-wins actively wrong, because a placement that answers
 * `isSelf` for its own actor would then run that actor inside a DIFFERENT
 * object's execution. `@sigx/actors-cloudflare` enters the scope in every
 * handler it owns; a subclass handler that hops ambiently before delegating
 * to `super` (or a Worker route outside the handler) wraps itself.
 *
 * The scope is an `AsyncLocalStorage`, so it follows awaits, timers and
 * detached work started inside `fn`. Loaded lazily, like the reentrancy
 * store, and installed on `globalThis` so two module graphs share it; a
 * runtime without it runs `fn` unscoped, on the global.
 */
import type { Host } from '../types';
import type { HostScope, HostScopeRead } from '../seam';
import { loadCallStore } from './reentrancy';

interface ScopeGlobal {
    __SIGX_ACTOR_HOST_SCOPE__?: HostScope;
}

let warned = false;

async function hostScope(): Promise<HostScope | null> {
    const g = globalThis as ScopeGlobal;
    if (g.__SIGX_ACTOR_HOST_SCOPE__) return g.__SIGX_ACTOR_HOST_SCOPE__;
    let Store;
    try {
        Store = await loadCallStore();
    } catch {
        if (__DEV__ && !warned) {
            warned = true;
            console.warn(
                '[sigx actors] runWithHost needs AsyncLocalStorage (node:async_hooks), which ' +
                    'this runtime does not provide — ambient actor() falls back to the last ' +
                    'host started. On Cloudflare Workers enable `nodejs_compat`.'
            );
        }
        return null;
    }
    // Re-read after the await: a concurrent first call may have installed it.
    return (g.__SIGX_ACTOR_HOST_SCOPE__ ??= new Store<HostScopeRead>());
}

/**
 * Run `fn` with `host` as the ambient host. Pass a thunk to scope a host
 * that is still booting: while it answers `undefined`, reads fall back to
 * the stamped global.
 */
export async function runWithHost<R>(
    host: Host | HostScopeRead,
    fn: () => R | Promise<R>
): Promise<R> {
    const scope = await hostScope();
    if (!scope) return fn();
    const read: HostScopeRead = typeof host === 'function' ? host : () => host;
    return scope.run(read, fn);
}
