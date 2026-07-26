/**
 * `@sigx/actors/client` — the browser-side actor transport. Dependency-light
 * by contract (`@sigx/serialize` only, size-limited): the build transform
 * swaps a `*.actor.ts` module for `__actorRef(...)` calls importing this
 * entry, so its bytes ride every client bundle that touches an actor.
 *
 * One GENERIC Proxy serves every actor — no per-method stub compilation.
 * Method types come from the real module's declarations (the swap changes
 * values, never types).
 *
 * Wire contract (mirrors `@sigx/server/client`, pinned by integration tests
 * against the real endpoint): POST `{endpoint}/{Type}%23{method}` with
 * `{"args": [key, ...args]}` → `{"data"}` / `{"error"}` envelope, or NDJSON
 * `{"chunk"}* ({"done"}|{"error"})` for stream methods. Errors are
 * re-created with the `__sigxServerFnError` brand so `isServerFnError`
 * matches them. The parsing/branding plumbing lives in `../wire-shared` —
 * shared with the silo-to-silo transport.
 */
import {
    encodeWire,
    readNdjson,
    reviveWire,
    reviver,
    wireFail,
    type WireError
} from '../wire-shared';

// ---------------------------------------------------------------------------
// Transport config — deliberately SEPARATE from `configureServerFn`: a
// remote-backend app may point fn stubs and actor calls at different bases.

export interface ActorTransport {
    /** Absolute URL or path prefix; wins over the build-time endpoint. */
    endpoint?: string;
    /** Extra request headers — static map or (possibly async) factory. */
    headers?:
        | Record<string, string>
        | (() => Record<string, string> | Promise<Record<string, string>>);
    /** Fetch implementation; default is the global fetch. */
    fetch?: typeof globalThis.fetch;
}

let transport: ActorTransport | null = null;

/** Set (or with `null` clear) the transport every actor ref resolves at call time. */
export function configureActors(config: ActorTransport | null): void {
    transport = config;
}

// ---------------------------------------------------------------------------

export interface ActorClientCallOptions {
    signal?: AbortSignal;
    headers?: Record<string, string>;
}

async function send(
    endpoint: string,
    symbol: string,
    args: unknown[],
    options?: ActorClientCallOptions
): Promise<Response> {
    const config = transport;
    const target = config?.endpoint ?? endpoint;
    const prefix = target.endsWith('/') ? target.slice(0, -1) : target;
    const extra =
        typeof config?.headers === 'function' ? await config.headers() : config?.headers;
    // content-type is NOT overridable — the endpoint 415s anything else.
    const headers: Record<string, string> = {};
    for (const source of [extra, options?.headers]) {
        for (const key in source) {
            if (key.toLowerCase() !== 'content-type') headers[key] = source[key]!;
        }
    }
    headers['content-type'] = 'application/json';
    const init: RequestInit = {
        method: 'POST',
        headers,
        body: JSON.stringify({ args: encodeWire(args) }),
        ...(options?.signal ? { signal: options.signal } : {})
    };
    const url = `${prefix}/${encodeURIComponent(symbol)}`;
    return config?.fetch ? config.fetch(url, init) : fetch(url, init);
}

function skewHint(symbol: string, status: number): string {
    return status === 404
        ? `[sigx actors] the server does not know "${symbol}" — is the actor registered ` +
              `with the silo, and are client and server builds from the same deploy?`
        : `[sigx actors] call to "${symbol}" failed with HTTP ${status}`;
}

async function callUnary(
    endpoint: string,
    symbol: string,
    args: unknown[],
    options?: ActorClientCallOptions
): Promise<unknown> {
    const res = await send(endpoint, symbol, args, options);
    let parsed: { data?: unknown; error?: WireError } | undefined;
    try {
        parsed = JSON.parse(await res.text(), reviver) as { data?: unknown; error?: WireError };
    } catch {
        parsed = undefined;
    }
    if (!res.ok || !parsed || parsed.error) {
        throw wireFail(res.status, parsed?.error, skewHint(symbol, res.status));
    }
    return reviveWire(parsed.data);
}

function callStream(
    endpoint: string,
    symbol: string,
    args: unknown[],
    options?: ActorClientCallOptions
): AsyncIterable<unknown> {
    const controller = new AbortController();
    const signal = options?.signal
        ? AbortSignal.any([options.signal, controller.signal])
        : controller.signal;
    async function* stream(): AsyncGenerator<unknown> {
        try {
            const res = await send(endpoint, symbol, args, { ...options, signal });
            if (!res.ok || !res.body) {
                let wire: WireError | undefined;
                try {
                    wire = (JSON.parse(await res.text(), reviver) as { error?: WireError })
                        ?.error;
                } catch {
                    wire = undefined;
                }
                throw wireFail(res.status, wire, skewHint(symbol, res.status));
            }
            yield* readNdjson(res, symbol);
        } finally {
            controller.abort(); // consumer break/return, error, or normal end
        }
    }
    return stream();
}

// ---------------------------------------------------------------------------

/**
 * The client-side actor ref the build transform emits for each exported
 * `defineActor`. Carries the `__sigxActorProxy` brand that `actor()` from
 * the root entry branches on.
 */
export function __actorRef(
    type: string,
    endpoint: string,
    streams: readonly string[] = []
): object {
    const streamNames = new Set(streams);
    const makeProxy = (key: string, options?: ActorClientCallOptions): object => {
        const cache = new Map<string | symbol, unknown>();
        return new Proxy(Object.create(null) as object, {
            get(_target, prop) {
                if (typeof prop === 'symbol') return undefined;
                if (prop === 'then') return undefined;
                const hit = cache.get(prop);
                if (hit) return hit;
                const symbol = `${type}#${prop}`;
                let member: unknown;
                if (prop === 'with') {
                    member = (next?: ActorClientCallOptions) => makeProxy(key, next);
                } else if (streamNames.has(prop)) {
                    member = (...args: unknown[]) =>
                        callStream(endpoint, symbol, [key, ...args], options);
                } else {
                    member = (...args: unknown[]) =>
                        callUnary(endpoint, symbol, [key, ...args], options);
                }
                cache.set(prop, member);
                return member;
            }
        });
    };
    return {
        __sigxActor: type,
        __sigxActorProxy: makeProxy
    };
}
