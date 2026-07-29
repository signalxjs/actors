/**
 * The list-then-watch loop every Kubernetes controller runs, reduced to
 * what a membership view needs: LIST seeds the lease map and yields a
 * resourceVersion; WATCH streams deltas from there; 410 Gone (the server
 * compacted past our resourceVersion) re-lists immediately; a dropped
 * stream re-lists after backoff. resourceVersion is treated as the opaque
 * bookmark the API conventions say it is — never parsed, never compared.
 */
import type { KubeClient } from './client';
import type { LeaseObject } from './lease';

export interface WatchEvent {
    type: 'ADDED' | 'MODIFIED' | 'DELETED' | 'BOOKMARK' | 'ERROR';
    object: LeaseObject & { code?: number };
}

/** Where the loop lands its data — the membership's lease map. */
export interface ListWatchTarget {
    /** Full replacement from a LIST snapshot. */
    replaceAll(leases: readonly LeaseObject[]): void;
    /** One watch delta. */
    apply(type: 'ADDED' | 'MODIFIED' | 'DELETED', lease: LeaseObject): void;
}

export interface ListWatchOptions {
    client: KubeClient;
    labelSelector: string;
    target: ListWatchTarget;
    backoff: { minMs: number; maxMs: number };
    /** Server-side watch timeout — a clean end + re-list on this cadence. */
    timeoutSeconds?: number;
    /** Failures are routed here (staleness, never fatal); default silent. */
    onError?: (error: unknown) => void;
}

export interface ListWatchLoop {
    start(): void;
    stop(): void;
    /**
     * Resync now: interrupt the stream, LIST, and resume watching from the
     * fresh snapshot's resourceVersion — `refresh()` and the relist safety
     * net. Running the LIST beside a live stream instead would let an older
     * snapshot clobber state the watch had already delivered; restarting
     * the watch at the snapshot makes the interleaving impossible.
     * Resolves after the LIST lands; rejects if it fails.
     */
    relist(): Promise<void>;
}

/** Internal marker: the watch's resourceVersion is gone; re-list at once. */
const GONE = Symbol('gone');

const noop = (): void => {};

export async function* ndjsonLines(body: ReadableStream<Uint8Array>): AsyncGenerator<unknown> {
    const reader = body.getReader();
    const decoder = new TextDecoder();
    let buffer = '';
    try {
        for (;;) {
            const { done, value } = await reader.read();
            if (done) break;
            buffer += decoder.decode(value, { stream: true });
            let newline;
            while ((newline = buffer.indexOf('\n')) !== -1) {
                const line = buffer.slice(0, newline).trim();
                buffer = buffer.slice(newline + 1);
                if (line) yield JSON.parse(line);
            }
        }
        // A final line missing its newline is still an event.
        buffer += decoder.decode();
        const tail = buffer.trim();
        if (tail) yield JSON.parse(tail);
    } finally {
        await reader.cancel().catch(noop);
    }
}

export function listWatchLoop(options: ListWatchOptions): ListWatchLoop {
    const { client, labelSelector, target, backoff } = options;
    const timeoutSeconds = options.timeoutSeconds ?? 240;
    const onError = options.onError ?? noop;

    let stopped = false;
    let controller: AbortController | null = null;
    let forceList = false;
    let waiters: Array<{ resolve: () => void; reject: (error: unknown) => void }> = [];

    const settleWaiters = (error?: unknown): void => {
        const settled = waiters;
        waiters = [];
        for (const waiter of settled) {
            if (error === undefined) waiter.resolve();
            else waiter.reject(error);
        }
    };

    const list = async (signal?: AbortSignal): Promise<string> => {
        const res = await client.list({ query: { labelSelector }, signal });
        if (!res.ok) {
            res.body?.cancel().catch(noop);
            throw new Error(`lease list failed: HTTP ${res.status}`);
        }
        const body = (await res.json()) as {
            metadata?: { resourceVersion?: string };
            items?: LeaseObject[];
        };
        target.replaceAll(body.items ?? []);
        return body.metadata?.resourceVersion ?? '';
    };

    /** Streams from `resourceVersion`; returns the last one seen on clean end. */
    const watchFrom = async (resourceVersion: string, signal: AbortSignal): Promise<string> => {
        const res = await client.watch({
            query: {
                watch: '1',
                labelSelector,
                resourceVersion,
                allowWatchBookmarks: 'true',
                timeoutSeconds: String(timeoutSeconds)
            },
            signal
        });
        if (res.status === 410) {
            res.body?.cancel().catch(noop);
            throw GONE;
        }
        if (!res.ok || !res.body) {
            res.body?.cancel().catch(noop);
            throw new Error(`lease watch failed: HTTP ${res.status}`);
        }
        let seen = resourceVersion;
        for await (const raw of ndjsonLines(res.body)) {
            const event = raw as WatchEvent;
            if (event.type === 'ERROR') {
                throw event.object?.code === 410 ? GONE : new Error('lease watch error event');
            }
            seen = event.object?.metadata?.resourceVersion ?? seen;
            if (event.type === 'BOOKMARK') continue;
            target.apply(event.type, event.object);
        }
        // Clean end: the server-side timeout lapsed.
        return seen;
    };

    const run = async (): Promise<void> => {
        let delayMs = backoff.minMs;
        let resourceVersion = '';
        while (!stopped) {
            controller = new AbortController();
            try {
                // Re-list only when there is no bookmark to resume from —
                // first cycle, after 410 Gone, and on a requested resync.
                // Otherwise the watch picks up where the last stream left off.
                if (forceList) {
                    forceList = false;
                    resourceVersion = '';
                }
                if (!resourceVersion) {
                    resourceVersion = await list(controller.signal);
                    settleWaiters();
                }
                delayMs = backoff.minMs;
                // A relist() that landed while the LIST above was in flight
                // has already aborted this cycle's controller — starting the
                // watch on a spent signal would make it un-interruptible.
                if (forceList || controller.signal.aborted) continue;
                resourceVersion = await watchFrom(resourceVersion, controller.signal);
            } catch (error) {
                if (stopped) {
                    settleWaiters();
                    return;
                }
                if (forceList) continue; // deliberate interrupt for a resync
                if (error === GONE) {
                    resourceVersion = '';
                    continue;
                }
                settleWaiters(error); // a pending relist() sees the failure
                onError(error);
                await new Promise((resolve) => {
                    const timer = setTimeout(resolve, delayMs / 2 + Math.random() * (delayMs / 2));
                    (timer as { unref?: () => void }).unref?.();
                });
                delayMs = Math.min(delayMs * 2, backoff.maxMs);
            }
        }
    };

    return {
        start() {
            void run();
        },
        stop() {
            stopped = true;
            controller?.abort();
            settleWaiters();
        },
        relist() {
            if (stopped) return Promise.resolve();
            return new Promise<void>((resolve, reject) => {
                waiters.push({ resolve, reject });
                forceList = true;
                controller?.abort();
            });
        }
    };
}
