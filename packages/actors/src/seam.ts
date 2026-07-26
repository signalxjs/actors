/**
 * `__SIGX_ACTOR_SILO__` — the one global seam this package owns.
 *
 * | Stamped by | `silo.start()` (re-stamped per start; cleared by `stop()`) |
 * | Read by    | `currentSilo()` below — the single accessor               |
 * | Contract   | the `Silo` interface from `./types`                        |
 *
 * Why a global and not DI or a module-level instance: server functions run
 * app-less at the endpoint (no app context to resolve a token from), and in
 * dev the Vite module runner and Node hold two copies of the same module —
 * `globalThis` is the one store both graphs share (the hazard core documents
 * on `__SIGX_SERVERFN_CONTEXT__`).
 *
 * DELIBERATE deviation from the seams-doc "missing seam = no-op" rule:
 * writer and reader are the same package, so absence is not "the other pack
 * isn't installed" — it is "no silo is running", a wiring bug the author
 * must see. Fail closed, loudly.
 */
import type { Silo } from './types';

interface SiloGlobal {
    __SIGX_ACTOR_SILO__?: Silo;
}

/** The single accessor. Throws (descriptively) when no silo is running. */
export function currentSilo(): Silo {
    const silo = (globalThis as SiloGlobal).__SIGX_ACTOR_SILO__;
    if (!silo) {
        throw new Error(
            '[sigx actors] no silo is running — actor() needs one. Create and start it in ' +
                'your server entry:  const silo = createSilo({ actors, storage }); await silo.start();  ' +
                "(from '@sigx/actors/silo'). In dev, the @sigx/actors/vite plugin starts one for you."
        );
    }
    return silo;
}

/** Peek without throwing — for capability checks, not for dispatch. */
export function peekSilo(): Silo | undefined {
    return (globalThis as SiloGlobal).__SIGX_ACTOR_SILO__;
}

/** @internal stamped by `silo.start()`. Last-wins with a dev warning. */
export function stampSilo(silo: Silo): void {
    const g = globalThis as SiloGlobal;
    if (__DEV__ && g.__SIGX_ACTOR_SILO__ && g.__SIGX_ACTOR_SILO__ !== silo) {
        console.warn(
            '[sigx actors] a second silo was started while one is already running; ' +
                'the new one wins. Stop the old silo first unless this is a dev-server restart.'
        );
    }
    g.__SIGX_ACTOR_SILO__ = silo;
}

/** @internal cleared by `silo.stop()` — only if this silo still owns it. */
export function clearSilo(silo: Silo): void {
    const g = globalThis as SiloGlobal;
    if (g.__SIGX_ACTOR_SILO__ === silo) delete g.__SIGX_ACTOR_SILO__;
}
