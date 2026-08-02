/**
 * Silo setup for benchmarks.
 *
 * Two decisions worth stating, because they change the numbers:
 *
 * 1. `manualScheduler()` by default. The sweeper and the reminder tick are
 *    real background work; leaving them on the wall clock means a scenario
 *    occasionally eats a 100k-activation directory scan mid-measurement and
 *    reports it as latency. Scenarios that MEASURE that work drive the
 *    manual clock explicitly instead.
 *
 * 2. `callTimeoutMs` is a parameter, not a constant. The production default
 *    is 30s, and a non-zero deadline routes every dispatch through the
 *    call-deadline registry (`CallDeadlines`). Benchmarking only the `0`
 *    case would flatter the runtime; benchmarking only the default would
 *    hide what the deadline costs. So the dispatch scenarios run both.
 */
import { createSilo, manualScheduler, memoryStorage } from '@sigx/actors/silo';
import type {
    ActorCallContext,
    ActorRef,
    ActorStorage,
    ManualScheduler,
    Silo,
    SiloDefaults
} from '@sigx/actors/silo';
import type { AnyActorDefinition } from '@sigx/actors';

/** Production default; a per-call deadline race is paid on every dispatch. */
export const PRODUCTION_CALL_TIMEOUT_MS = 30_000;

export interface SiloFixture {
    silo: Silo;
    storage: ActorStorage;
    clock: ManualScheduler;
    stop(): Promise<void>;
}

export interface SiloFixtureOptions {
    actors: readonly AnyActorDefinition[];
    storage?: ActorStorage;
    /** Defaults to 0 — no deadline machinery on the measured path. */
    callTimeoutMs?: number;
    defaults?: SiloDefaults;
}

export async function createBenchSilo(options: SiloFixtureOptions): Promise<SiloFixture> {
    const clock = manualScheduler();
    const storage = options.storage ?? memoryStorage();
    const silo = createSilo({
        actors: [...options.actors],
        storage,
        scheduler: clock,
        defaults: {
            // Long enough that nothing collects mid-run; the sweeper scenario
            // calls the sweep path directly rather than waiting for it.
            idleAfterMs: 3_600_000,
            sweepIntervalMs: 3_600_000,
            reminderTickMs: 3_600_000,
            callTimeoutMs: options.callTimeoutMs ?? 0,
            devSerializeChecks: false,
            ...options.defaults
        }
    });
    await silo.start();
    return {
        silo,
        storage,
        clock,
        stop: () => silo.stop({ timeoutMs: 5_000 })
    };
}

/**
 * A call context identical to the one the wire endpoint mints, minus the
 * per-call `mintCallId()`. Reused across a whole loop on purpose: the raw
 * dispatch scenario is measuring the dispatcher, and allocating a fresh
 * context per call would fold the caller's cost into the callee's number.
 * `dispatch/via-proxy` is where that cost gets priced.
 */
export function benchCall(overrides: Partial<ActorCallContext> = {}): ActorCallContext {
    return { callChain: [], callId: 'bench', ...overrides };
}

/**
 * `Silo.dispatchStream` is optional on the public type — a dispatch
 * middleware that returns a bare `{ dispatch }` silently drops it, which is
 * a real failure mode the runtime has a dev warning for. A non-null
 * assertion here would surface that as `undefined is not a function`
 * somewhere inside a benchmark; this says what actually happened.
 */
export function requireStreamDispatch(silo: Silo): NonNullable<Silo['dispatchStream']> {
    if (!silo.dispatchStream) {
        throw new Error(
            'silo.dispatchStream is missing — a dispatch middleware is not forwarding it, ' +
                'so no `streams:` method can run.'
        );
    }
    return silo.dispatchStream.bind(silo);
}

export function refsFor(type: string, count: number, prefix = 'k'): ActorRef[] {
    return Array.from({ length: count }, (_, i) => ({ type, key: `${prefix}${i}` }));
}

/** Activate `refs` so a measurement starts against warm activations. */
export async function warmActivations(
    silo: Silo,
    refs: readonly ActorRef[],
    method = 'noop'
): Promise<void> {
    const call = benchCall();
    // Sequential: 100k concurrent activations would measure the activation
    // storm rather than warming for the thing we actually want to measure.
    for (const ref of refs) {
        await silo.dispatch(ref, method, [], call);
    }
}
