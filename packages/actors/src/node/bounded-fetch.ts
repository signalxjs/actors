/**
 * `boundedFetch` — a fetch whose undici pool is CAPPED, built for the
 * cluster's `fetch` seam (#118).
 *
 * Node's global `fetch` runs on an UNBOUNDED undici pool, measured at two
 * sockets per in-flight request against a peer (`benchmarks/BASELINES.md`,
 * Tier 2) — at concurrency 64 across 99 peers that projects to ~12 600
 * sockets per host. Capping the pool AT the concurrency halves that and is
 * marginally faster; capping it far below is a −66% throughput trap. So
 * `connections` is REQUIRED and comes from the caller — match your host's
 * outbound concurrency — never a tuned constant baked in here: the sweet
 * spot moves across undici majors (Node 20/22 bundle 6.x, Node 24 ships
 * 7.x), and a constant measured on one would quietly mis-tune the others.
 *
 * This lives on `./node`, not `./cluster`, deliberately: the cluster entry
 * stays WinterCG-clean, and undici's Dispatcher API is only reachable from
 * the installed `undici` package (`node:undici` is not a builtin, and the
 * bundled copy behind global fetch exposes no import). Hence the lazy
 * import of the OPTIONAL `undici` peer — a host that never calls the
 * returned fetch never pays for it, and one without the package installed
 * gets guidance instead of a bare MODULE_NOT_FOUND.
 *
 * Scoped, not global: the agent is threaded per call as `dispatcher`, so
 * only the fetch returned here is re-tuned — never the app's own outbound
 * HTTP or another SDK's. Re-binding undici's global dispatcher would reach
 * the same numbers and is exactly what a library must not do to its host.
 *
 * ```ts
 * import { boundedFetch } from '@sigx/actors/node';
 * cluster({ providers, advertise, secret, fetch: boundedFetch({ connections: 64 }) });
 * ```
 */

/**
 * The slice of undici this helper uses. Structural, so the module
 * typechecks and bundles without undici's types installed — and so tests
 * can inject a fake through {@link createBoundedFetch}.
 */
export interface UndiciLike {
    Agent: new (options: { connections: number }) => object;
    fetch: (
        input: string | URL | Request,
        init?: RequestInit & { dispatcher?: object }
    ) => Promise<Response>;
}

export interface BoundedFetchOptions {
    /**
     * Socket cap per origin — MATCH YOUR CONCURRENCY. At the host's
     * outbound concurrency the cap halves the socket count at no cost;
     * well below it, calls queue behind too few sockets and throughput
     * collapses. There is no safe default, so there is no default.
     */
    connections: number;
}

function loadUndici(): Promise<UndiciLike> {
    // The specifier rides a variable so the optional peer stays out of the
    // bundle and out of the type graph (same pattern as `loadCallStore`).
    const specifier = 'undici';
    return import(/* @vite-ignore */ specifier) as Promise<UndiciLike>;
}

/**
 * Internal factory behind {@link boundedFetch}: same contract, with the
 * undici loader injectable. Exported for the unit tests only — it is not on
 * the `@sigx/actors/node` entry, and the loader parameter is not API.
 */
export function createBoundedFetch(
    options: BoundedFetchOptions,
    load: () => Promise<UndiciLike>
): typeof globalThis.fetch {
    const { connections } = options;
    if (!Number.isInteger(connections) || connections < 1) {
        throw new Error(
            `[sigx actors] boundedFetch: \`connections\` must be a positive integer ` +
                `(got ${connections}) — match your host's outbound concurrency.`
        );
    }

    // One agent per helper, shared by every call — that sharing IS the cap.
    // The promise is cached so concurrent first calls load once; a rejection
    // clears it so the guidance error stays throwable rather than a
    // one-shot, without caching the failure forever.
    let ready: Promise<{ undici: UndiciLike; agent: object }> | undefined;
    function agentReady() {
        ready ??= load().then(
            (undici) => ({ undici, agent: new undici.Agent({ connections }) }),
            (error) => {
                ready = undefined;
                const code = (error as { code?: string } | null)?.code;
                if (code === 'ERR_MODULE_NOT_FOUND' || code === 'MODULE_NOT_FOUND') {
                    throw new Error(
                        `[sigx actors] boundedFetch needs the optional peer dependency ` +
                            `"undici" — install it next to @sigx/actors (e.g. \`pnpm add undici\`). ` +
                            `Node bundles undici for its global fetch but does not expose the ` +
                            `Dispatcher API, so the package itself is required to cap the pool.`
                    );
                }
                throw error;
            }
        );
        return ready;
    }

    return async function fetch(input, init) {
        const { undici, agent } = await agentReady();
        return undici.fetch(input as string | URL | Request, { ...init, dispatcher: agent });
    };
}

/**
 * A `fetch` that forwards every call through one undici
 * `Agent({ connections })` — hand it to `cluster({ fetch })` (or any other
 * fetch seam) to bound that seam's socket pool without touching the
 * process's global dispatcher. Requires the optional `undici` peer; loaded
 * lazily on the first call.
 */
export function boundedFetch(options: BoundedFetchOptions): typeof globalThis.fetch {
    return createBoundedFetch(options, loadUndici);
}
