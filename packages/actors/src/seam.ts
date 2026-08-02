/**
 * `__SIGX_ACTOR_HOST__` — the one global seam this package owns.
 *
 * | Stamped by | `host.start()` (re-stamped per start; cleared by `stop()`) |
 * | Read by    | `currentHost()` below — the single accessor               |
 * | Contract   | the `Host` interface from `./types`                        |
 *
 * Why a global and not DI or a module-level instance: server functions run
 * app-less at the endpoint (no app context to resolve a token from), and in
 * dev the Vite module runner and Node hold two copies of the same module —
 * `globalThis` is the one store both graphs share (the hazard core documents
 * on `__SIGX_SERVERFN_CONTEXT__`).
 *
 * DELIBERATE deviation from the seams-doc "missing seam = no-op" rule:
 * writer and reader are the same package, so absence is not "the other pack
 * isn't installed" — it is "no host is running", a wiring bug the author
 * must see. Fail closed, loudly.
 */
import type { Host } from './types';

interface HostGlobal {
    __SIGX_ACTOR_HOST__?: Host;
}

/** The single accessor. Throws (descriptively) when no host is running. */
export function currentHost(): Host {
    const host = (globalThis as HostGlobal).__SIGX_ACTOR_HOST__;
    if (!host) {
        throw new Error(
            '[sigx actors] no host is running — actor() needs one. Create and start it in ' +
                'your server entry:  const host = createHost({ actors, storage }); await host.start();  ' +
                "(from '@sigx/actors/host'). In dev, the @sigx/actors/vite plugin starts one for you."
        );
    }
    return host;
}

/** Peek without throwing — for capability checks, not for dispatch. */
export function peekHost(): Host | undefined {
    return (globalThis as HostGlobal).__SIGX_ACTOR_HOST__;
}

/** @internal stamped by `host.start()`. Last-wins with a dev warning. */
export function stampHost(host: Host): void {
    const g = globalThis as HostGlobal;
    if (__DEV__ && g.__SIGX_ACTOR_HOST__ && g.__SIGX_ACTOR_HOST__ !== host) {
        console.warn(
            '[sigx actors] a second host was started while one is already running; ' +
                'the new one wins. Stop the old host first unless this is a dev-server restart.'
        );
    }
    g.__SIGX_ACTOR_HOST__ = host;
}

/** @internal cleared by `host.stop()` — only if this host still owns it. */
export function clearHost(host: Host): void {
    const g = globalThis as HostGlobal;
    if (g.__SIGX_ACTOR_HOST__ === host) delete g.__SIGX_ACTOR_HOST__;
}
