/**
 * A Lease-scoped Kubernetes API client over plain fetch. No client library:
 * the coordination.k8s.io surface this package touches is six verbs on one
 * resource, and a client lib would be the package's only dependency.
 *
 * In-cluster defaults come from the pod's ServiceAccount mount (token, CA,
 * namespace). The token is re-read on a cadence and on 401 — bound
 * ServiceAccount tokens rotate. Outside the cluster, inject `apiServer`
 * (e.g. `kubectl proxy`) and/or `fetch`.
 */
import { readFileSync } from 'node:fs';
import { Readable } from 'node:stream';

const SA_DIR = '/var/run/secrets/kubernetes.io/serviceaccount';
const TOKEN_REFRESH_MS = 60_000;

export interface KubeClientOptions {
    /** API server origin. Default: `KUBERNETES_SERVICE_HOST`/`_PORT` env, else `https://kubernetes.default.svc`. */
    apiServer?: string;
    /** Namespace. Default: the ServiceAccount namespace file, else `default`. */
    namespace?: string;
    /** Bearer token or provider. Default: the ServiceAccount token file. */
    token?: string | (() => string | Promise<string>);
    /** PEM CA bundle for the API server. Default: the ServiceAccount ca.crt. */
    ca?: string;
    /** Transport override (tests, `kubectl proxy` dev). Skips token/CA defaults. */
    fetch?: typeof fetch;
}

export interface LeaseRequestOptions {
    query?: Record<string, string>;
    signal?: AbortSignal;
}

/** The verbs membership and the watch loop need, namespace-bound. */
export interface KubeClient {
    readonly namespace: string;
    get(name: string, options?: LeaseRequestOptions): Promise<Response>;
    create(body: unknown, options?: LeaseRequestOptions): Promise<Response>;
    replace(name: string, body: unknown, options?: LeaseRequestOptions): Promise<Response>;
    /** JSON merge-patch (`application/merge-patch+json`). */
    patch(name: string, body: unknown, options?: LeaseRequestOptions): Promise<Response>;
    delete(name: string, options?: LeaseRequestOptions): Promise<Response>;
    list(options?: LeaseRequestOptions): Promise<Response>;
    /** Streaming watch — the returned Response body is the event stream. */
    watch(options?: LeaseRequestOptions): Promise<Response>;
}

function readOptional(path: string): string | undefined {
    try {
        return readFileSync(path, 'utf8');
    } catch {
        return undefined;
    }
}

function defaultApiServer(): string {
    const host = process.env.KUBERNETES_SERVICE_HOST;
    const port = process.env.KUBERNETES_SERVICE_PORT ?? '443';
    return host ? `https://${host}:${port}` : 'https://kubernetes.default.svc';
}

/**
 * `fetch` cannot take a private CA, and undici (which could) is not an
 * importable built-in — so the default transport is node:http(s) wrapped to
 * the fetch signature. Streaming falls out of `Readable.toWeb`.
 */
function nodeFetch(ca: string | undefined): typeof fetch {
    return (input, init = {}) =>
        new Promise<Response>((resolve, reject) => {
            const url = new URL(input instanceof Request ? input.url : String(input));
            const https = url.protocol === 'https:';
            void (https ? import('node:https') : import('node:http')).then((mod) => {
                if (init.signal?.aborted) {
                    reject(new DOMException('The operation was aborted.', 'AbortError'));
                    return;
                }
                const req = mod.request(
                    url,
                    {
                        method: init.method ?? 'GET',
                        headers: init.headers as Record<string, string>,
                        ...(https && ca ? { ca } : {})
                    },
                    (res) => {
                        const status = res.statusCode ?? 500;
                        const headers = new Headers();
                        for (const [key, value] of Object.entries(res.headers)) {
                            if (typeof value === 'string') headers.set(key, value);
                        }
                        const body =
                            status === 204 || status === 304
                                ? null
                                : (Readable.toWeb(res) as unknown as BodyInit);
                        resolve(new Response(body, { status, headers }));
                    }
                );
                req.on('error', reject);
                init.signal?.addEventListener('abort', () => {
                    req.destroy(new DOMException('The operation was aborted.', 'AbortError'));
                });
                if (init.body !== undefined && init.body !== null) req.write(init.body);
                req.end();
            }, reject);
        });
}

export function kubeClient(options: KubeClientOptions = {}): KubeClient {
    const apiServer = (options.apiServer ?? defaultApiServer()).replace(/\/$/, '');
    const namespace =
        options.namespace ?? readOptional(`${SA_DIR}/namespace`)?.trim() ?? 'default';
    const transport =
        options.fetch ?? nodeFetch(options.ca ?? readOptional(`${SA_DIR}/ca.crt`));

    // Token resolution: a fixed string, a user provider (called per refresh),
    // or the ServiceAccount file. Cached; invalidated on 401 and on age.
    let cachedToken: string | null | undefined;
    let tokenReadMs = 0;
    const readToken = async (): Promise<string | null> => {
        const now = Date.now();
        if (cachedToken !== undefined && now - tokenReadMs < TOKEN_REFRESH_MS) {
            return cachedToken;
        }
        if (typeof options.token === 'string') cachedToken = options.token;
        else if (options.token) cachedToken = await options.token();
        else if (options.fetch) cachedToken = null;
        else cachedToken = readOptional(`${SA_DIR}/token`)?.trim() ?? null;
        tokenReadMs = now;
        return cachedToken;
    };

    const base = `${apiServer}/apis/coordination.k8s.io/v1/namespaces/${encodeURIComponent(namespace)}/leases`;

    const request = async (
        method: string,
        path: string,
        body: unknown,
        contentType: string | undefined,
        options_: LeaseRequestOptions = {}
    ): Promise<Response> => {
        const query = options_.query
            ? `?${new URLSearchParams(options_.query).toString()}`
            : '';
        const send = async (): Promise<Response> => {
            const token = await readToken();
            const headers: Record<string, string> = { accept: 'application/json' };
            if (token) headers.authorization = `Bearer ${token}`;
            if (contentType) headers['content-type'] = contentType;
            return transport(`${base}${path}${query}`, {
                method,
                headers,
                body: body === undefined ? undefined : JSON.stringify(body),
                signal: options_.signal
            });
        };
        const res = await send();
        if (res.status !== 401) return res;
        // Bound tokens rotate: drop the cache and retry once with a fresh read.
        res.body?.cancel().catch(() => {});
        cachedToken = undefined;
        return send();
    };

    return {
        namespace,
        get: (name, o) => request('GET', `/${encodeURIComponent(name)}`, undefined, undefined, o),
        create: (body, o) => request('POST', '', body, 'application/json', o),
        replace: (name, body, o) =>
            request('PUT', `/${encodeURIComponent(name)}`, body, 'application/json', o),
        patch: (name, body, o) =>
            request('PATCH', `/${encodeURIComponent(name)}`, body, 'application/merge-patch+json', o),
        delete: (name, o) =>
            request('DELETE', `/${encodeURIComponent(name)}`, undefined, undefined, o),
        list: (o) => request('GET', '', undefined, undefined, o),
        watch: (o) => request('GET', '', undefined, undefined, o)
    };
}
