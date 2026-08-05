/**
 * The benchmark suite's server app — imported for its SIDE EFFECT by any
 * scenario that drives a request path.
 *
 * The runtime is fail-closed since core 0.15 (rfc-server-v4): with no app
 * stamped in the process, every operation that does not declare
 * `allowAnonymous: true` answers 401 before it dispatches. The bench
 * fixtures are bare by design — they measure the runtime, not an
 * application — so without this the `wire/guard-chain` scenario simply
 * THROWS, and a scenario that throws fails the Bench step outright,
 * `exact` metrics or not.
 *
 * Stamping one is also the more honest measurement. A real deployment has
 * an app; benchmarking the unconfigured process would measure a shape
 * nobody ships. The authenticator is O(1) and the codec is the identity
 * function, so what the numbers still contain is the framework's own
 * per-request pipeline cost — config resolution, the middleware slot, the
 * principal memo, the identity gate, `requireAuthenticated` — and nothing
 * of the app's.
 *
 * Mirrors what core does for its own request-path harnesses
 * (`benchmarks/src/micro/app.ts`, signalxjs/core#618).
 */
import { createServerApp } from '@sigx/server/server';

export interface BenchPrincipal {
    readonly id: string;
}

export const benchApp = createServerApp<BenchPrincipal>({
    authenticate: () => ({ id: 'bench' }),
    codec: {
        encode: (principal) => principal.id,
        decode: (encoded) => (encoded === '' ? null : { id: encoded })
    }
});
