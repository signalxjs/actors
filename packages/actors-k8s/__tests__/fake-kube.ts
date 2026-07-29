/**
 * A fake Kubernetes API server for the Lease surface, as an injectable
 * `fetch` — no sockets. Hand-written behavior rather than recorded
 * fixtures, because the interesting cases are the faults: dropped watch
 * streams, compacted resourceVersions (410 Gone), failed renewals,
 * expired tokens.
 */
import type { LeaseObject } from '../src/lease';

interface StoredEvent {
    rv: number;
    type: 'ADDED' | 'MODIFIED' | 'DELETED';
    object: LeaseObject;
}

interface OpenWatch {
    labelSelector: string;
    controller: ReadableStreamDefaultController<Uint8Array>;
}

export interface FakeKube {
    fetch: typeof fetch;
    /** Lease store, name → object (live references; snapshot on read). */
    readonly leases: Map<string, LeaseObject>;
    /** End every open watch stream (network blip / API server restart). */
    dropWatches(): void;
    /** Send a BOOKMARK event carrying the current resourceVersion. */
    bookmark(): void;
    /** Forget history: watches from any older resourceVersion get 410 Gone. */
    compact(): void;
    /** Fail the next `n` PATCH renewals with HTTP 500. */
    failRenewals(n: number): void;
    /** Refuse (HTTP 500) new watch requests while active — holds a
     *  reconnecting client down so a test can mutate during the gap. */
    blockWatches(active?: boolean): void;
    /** Fail ALL requests until cleared with `failAll(0)`. */
    failAll(active?: boolean): void;
    /** When set, requests must carry `Bearer <validToken>` or get 401. */
    validToken: string | null;
    pendingWatchCount(): number;
    /** Requests seen, newest last: `PATCH sigx-s.abc`. */
    readonly log: string[];
}

const encoder = new TextEncoder();

function matchesSelector(lease: LeaseObject, selector: string): boolean {
    if (!selector) return true;
    return selector.split(',').every((clause) => {
        const [key, value] = clause.split('=');
        return lease.metadata.labels?.[key!] === value;
    });
}

function clone<T>(value: T): T {
    return JSON.parse(JSON.stringify(value)) as T;
}

export function fakeKube(namespace = 'test'): FakeKube {
    const leases = new Map<string, LeaseObject>();
    const events: StoredEvent[] = [];
    const watches = new Set<OpenWatch>();
    const log: string[] = [];
    let rv = 100;
    let compactedBefore = 0;
    let renewFailures = 0;
    let allFailing = false;
    let watchesBlocked = false;

    const base = `/apis/coordination.k8s.io/v1/namespaces/${namespace}/leases`;

    const record = (type: StoredEvent['type'], object: LeaseObject): void => {
        rv += 1;
        object.metadata.resourceVersion = String(rv);
        const event: StoredEvent = { rv, type, object: clone(object) };
        events.push(event);
        for (const watch of watches) {
            if (!matchesSelector(event.object, watch.labelSelector)) continue;
            watch.controller.enqueue(
                encoder.encode(`${JSON.stringify({ type, object: event.object })}\n`)
            );
        }
    };

    const json = (status: number, body: unknown): Response =>
        new Response(JSON.stringify(body), {
            status,
            headers: { 'content-type': 'application/json' }
        });

    const status = (code: number, reason: string): Response =>
        json(code, { kind: 'Status', code, reason });

    const fake: FakeKube = {
        leases,
        validToken: null,
        log,
        dropWatches() {
            for (const watch of watches) {
                try {
                    watch.controller.close();
                } catch {
                    /* already closed */
                }
            }
            watches.clear();
        },
        bookmark() {
            for (const watch of watches) {
                watch.controller.enqueue(
                    encoder.encode(
                        `${JSON.stringify({
                            type: 'BOOKMARK',
                            object: { metadata: { resourceVersion: String(rv) }, spec: {} }
                        })}\n`
                    )
                );
            }
        },
        compact() {
            compactedBefore = rv;
            events.length = 0;
        },
        failRenewals(n) {
            renewFailures = n;
        },
        blockWatches(active = true) {
            watchesBlocked = active;
        },
        failAll(active = true) {
            allFailing = active;
        },
        pendingWatchCount: () => watches.size,
        fetch: (input, init = {}) => {
            // Real fetch rejects up front on a spent signal — so does the fake.
            if (init.signal?.aborted) {
                return Promise.reject(new DOMException('The operation was aborted.', 'AbortError'));
            }
            const url = new URL(input instanceof Request ? input.url : String(input), 'http://fake');
            const method = init.method ?? 'GET';
            const name = url.pathname.startsWith(`${base}/`)
                ? decodeURIComponent(url.pathname.slice(base.length + 1))
                : null;
            log.push(`${method} ${name ?? url.pathname}${url.search ? url.search : ''}`);

            if (allFailing) return Promise.resolve(status(500, 'fakeAllFailing'));
            if (!url.pathname.startsWith(base)) return Promise.resolve(status(404, 'NotFound'));
            if (fake.validToken !== null) {
                const auth = (init.headers as Record<string, string> | undefined)?.authorization;
                if (auth !== `Bearer ${fake.validToken}`) {
                    return Promise.resolve(status(401, 'Unauthorized'));
                }
            }

            const body = init.body ? (JSON.parse(String(init.body)) as LeaseObject) : null;
            const selector = url.searchParams.get('labelSelector') ?? '';

            if (method === 'GET' && name === null && url.searchParams.get('watch') === '1') {
                if (watchesBlocked) return Promise.resolve(status(500, 'fakeWatchBlocked'));
                const from = Number(url.searchParams.get('resourceVersion') ?? '0');
                if (from < compactedBefore) return Promise.resolve(status(410, 'Expired'));
                const backlog = events.filter(
                    (event) => event.rv > from && matchesSelector(event.object, selector)
                );
                let open: OpenWatch;
                const stream = new ReadableStream<Uint8Array>({
                    start(controller) {
                        open = { labelSelector: selector, controller };
                        for (const event of backlog) {
                            controller.enqueue(
                                encoder.encode(
                                    `${JSON.stringify({ type: event.type, object: event.object })}\n`
                                )
                            );
                        }
                        watches.add(open);
                        init.signal?.addEventListener('abort', () => {
                            if (!watches.delete(open)) return;
                            try {
                                controller.error(
                                    new DOMException('The operation was aborted.', 'AbortError')
                                );
                            } catch {
                                /* already closed */
                            }
                        });
                    },
                    cancel() {
                        watches.delete(open);
                    }
                });
                return Promise.resolve(new Response(stream, { status: 200 }));
            }

            if (method === 'GET' && name === null) {
                const items = [...leases.values()]
                    .filter((lease) => matchesSelector(lease, selector))
                    .map(clone);
                return Promise.resolve(
                    json(200, {
                        kind: 'LeaseList',
                        metadata: { resourceVersion: String(rv) },
                        items
                    })
                );
            }

            if (method === 'GET') {
                const lease = leases.get(name!);
                return Promise.resolve(lease ? json(200, clone(lease)) : status(404, 'NotFound'));
            }

            if (method === 'POST') {
                const created = clone(body!);
                if (leases.has(created.metadata.name)) {
                    return Promise.resolve(status(409, 'AlreadyExists'));
                }
                leases.set(created.metadata.name, created);
                record('ADDED', created);
                return Promise.resolve(json(201, clone(created)));
            }

            if (method === 'PUT') {
                const replaced = clone(body!);
                if (!leases.has(name!)) return Promise.resolve(status(404, 'NotFound'));
                leases.set(name!, replaced);
                record('MODIFIED', replaced);
                return Promise.resolve(json(200, clone(replaced)));
            }

            if (method === 'PATCH') {
                if (renewFailures > 0) {
                    renewFailures -= 1;
                    return Promise.resolve(status(500, 'fakeRenewFailure'));
                }
                const existing = leases.get(name!);
                if (!existing) return Promise.resolve(status(404, 'NotFound'));
                const patch = body as unknown as {
                    metadata?: { annotations?: Record<string, string> };
                    spec?: Record<string, unknown>;
                };
                if (patch.spec) Object.assign(existing.spec, patch.spec);
                if (patch.metadata?.annotations) {
                    existing.metadata.annotations = {
                        ...existing.metadata.annotations,
                        ...patch.metadata.annotations
                    };
                }
                record('MODIFIED', existing);
                return Promise.resolve(json(200, clone(existing)));
            }

            if (method === 'DELETE') {
                const existing = leases.get(name!);
                if (!existing) return Promise.resolve(status(404, 'NotFound'));
                leases.delete(name!);
                record('DELETED', existing);
                return Promise.resolve(json(200, { kind: 'Status', status: 'Success' }));
            }

            return Promise.resolve(status(405, 'MethodNotAllowed'));
        }
    };
    return fake;
}
