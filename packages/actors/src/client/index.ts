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
 * matches them.
 */
import {
    encodeWithHandlers,
    reviveWithHandlers,
    type TypeHandler
} from '@sigx/serialize';

// ---------------------------------------------------------------------------
// Codec binding — the same `__SIGX_SERVERFN_CODEC__` seam the serverFn wire
// uses (documented in core's docs/seams.md), read through this entry's one
// accessor. Actor payloads share the serverFn vocabulary by design: one
// `serverPlugin({ types })` registration covers both wires.

function extraHandlers(): readonly TypeHandler[] {
    const extra = (globalThis as { __SIGX_SERVERFN_CODEC__?: TypeHandler[] })
        .__SIGX_SERVERFN_CODEC__;
    return Array.isArray(extra) ? extra : [];
}

const encodeWire = (value: unknown): unknown => encodeWithHandlers(value, extraHandlers());
const reviveWire = (value: unknown): unknown => reviveWithHandlers(value, extraHandlers());

/** Prototype-pollution keys DROPPED from every parsed payload. */
const DANGEROUS_KEYS = new Set(['__proto__', 'constructor', 'prototype']);
const reviver = (key: string, value: unknown): unknown =>
    DANGEROUS_KEYS.has(key) ? undefined : value;

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

interface WireError {
    message?: string;
    status?: number;
    data?: unknown;
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

/** Re-create a wire error with the `__sigxServerFnError` brand. */
function wireFail(status: number, wire: WireError | undefined, message: string): Error {
    return Object.assign(new Error(wire?.message ?? message), {
        __sigxServerFnError: true,
        status: wire?.status ?? status,
        data: wire?.data !== undefined ? reviveWire(wire.data) : undefined
    });
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
            const reader = res.body.getReader();
            const decoder = new TextDecoder();
            let buffer = '';
            const parseLine = (
                text: string
            ): { chunk?: unknown; done?: number; error?: WireError } =>
                JSON.parse(text, reviver) as { chunk?: unknown; done?: number; error?: WireError };
            for (;;) {
                const { value, done } = await reader.read();
                if (done) break;
                buffer += decoder.decode(value, { stream: true });
                let nl;
                while ((nl = buffer.indexOf('\n')) >= 0) {
                    const text = buffer.slice(0, nl);
                    buffer = buffer.slice(nl + 1);
                    if (!text) continue;
                    const obj = parseLine(text);
                    if ('error' in obj) {
                        throw wireFail(500, obj.error, `[sigx actors] stream "${symbol}" failed`);
                    }
                    if ('done' in obj) return;
                    yield reviveWire(obj.chunk);
                }
            }
            // EOF: honor a final line missing its trailing newline (proxies
            // may strip it); a partial line is genuine truncation.
            buffer += decoder.decode();
            const tail = buffer.trim();
            if (tail) {
                let obj: { chunk?: unknown; done?: number; error?: WireError } | null = null;
                try {
                    obj = parseLine(tail);
                } catch {
                    obj = null;
                }
                if (obj) {
                    if ('error' in obj) {
                        throw wireFail(500, obj.error, `[sigx actors] stream "${symbol}" failed`);
                    }
                    if ('done' in obj) return;
                    yield reviveWire(obj.chunk);
                }
            }
            throw new Error(
                `[sigx actors] stream "${symbol}" ended without a done/error terminator ` +
                    `(connection lost?)`
            );
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
