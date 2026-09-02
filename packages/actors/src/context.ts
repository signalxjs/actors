/**
 * In-process ServerFnContext resolution for actor guards — mirrors core's
 * order: explicit (`.with({ context })`) → ambient → detached.
 *
 * The ambient read uses core's documented `__SIGX_SERVERFN_CONTEXT__` seam
 * (docs/seams.md: stamped by every scope entry, contract
 * `() => Request | Partial<ServerFnContext> | undefined`). Since core 0.14
 * (signalxjs/core#494) a scope resolves to a `Partial<ServerFnContext>`
 * whose `locals` IS the per-request store — it must be passed through by
 * reference, so a guard writing `rq.locals` here is visible to every other
 * call in the flow. A bare `Request` stays legal in the contract (an app may
 * stamp its own resolver), so both branches below are load-bearing. This
 * function is this package's single accessor for it. Missing seam = detached
 * context — a no-op, per the seam rules; only touching `rq.request` then
 * throws, exactly like core's own detached context.
 */
import type { ServerFnContext } from '@sigx/server';

type AmbientInit = Request | Partial<ServerFnContext> | undefined;

interface ContextGlobal {
    __SIGX_SERVERFN_CONTEXT__?: () => AmbientInit;
}

let _detachedSignal: AbortSignal | undefined;
function detachedSignal(): AbortSignal {
    // Lazy: workerd forbids new AbortController() at module scope.
    return (_detachedSignal ??= new AbortController().signal);
}

export function resolveServerContext(
    explicit?: Request | Partial<ServerFnContext>,
    signal?: AbortSignal
): ServerFnContext {
    const init =
        explicit ?? (globalThis as ContextGlobal).__SIGX_SERVERFN_CONTEXT__?.() ?? undefined;
    if (init instanceof Request) {
        const request = init;
        return {
            request,
            get url() {
                return new URL(request.url);
            },
            abortSignal: signal ?? request.signal,
            responseHeaders: new Headers(),
            status: inertStatus,
            locals: {}
        };
    }
    if (init && typeof init === 'object') {
        const partial = init as Partial<ServerFnContext>;
        return {
            get request(): Request {
                return partial.request ?? noRequest('rq.request');
            },
            get url(): URL {
                return partial.url ?? (partial.request ? new URL(partial.request.url) : noRequest('rq.url'));
            },
            abortSignal: signal ?? partial.abortSignal ?? partial.request?.signal ?? detachedSignal(),
            responseHeaders: partial.responseHeaders ?? new Headers(),
            status: partial.status ?? inertStatus,
            locals: partial.locals ?? {}
        };
    }
    return {
        get request(): Request {
            return noRequest('rq.request');
        },
        get url(): URL {
            return noRequest('rq.url');
        },
        abortSignal: signal ?? detachedSignal(),
        responseHeaders: new Headers(),
        status: inertStatus,
        locals: {}
    };
}

function inertStatus(code: number): void {
    if (__DEV__) {
        console.warn(
            `[sigx actors] rq.status(${code}) is inert on an in-process actor call — ` +
                `there is no HTTP response to affect.`
        );
    }
}

function noRequest(what: string): never {
    throw new Error(
        `[sigx actors] ${what} is not available on an in-process actor call — nothing ` +
            `supplied a request. Call inside a request scope (the document/fn handlers open ` +
            `one), or hand one in: actor(Def, key).with({ context: request }).method(…).`
    );
}
