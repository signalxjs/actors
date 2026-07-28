/**
 * Shared test harness: an N-silo in-process cluster with zero sockets.
 * Every silo gets a fake origin (`http://silo<i>.test`); the shared
 * `pipeFetch` routes silo-to-silo and public wire requests straight into
 * the target silo's real handlers, re-creating fetch's body-cancel-on-abort
 * link so stream cancellation behaves like production.
 *
 * HAZARD: `silo.start()` stamps the LAST-WINS `__SIGX_ACTOR_SILO__` global,
 * so multi-silo tests must always dispatch through `silo.actor()` /
 * `configureActors({ fetch })` pipes — never the ambient `actor()` on the
 * server path.
 */
import { expect } from 'vitest';
import type { ActorStorage, AnyActorDefinition, Silo } from '@sigx/actors';
import {
    defineActorApp,
    memoryStorage,
    type ActorApp,
    type ActorPlugin,
    type SiloDefaults
} from '@sigx/actors/silo';
import { handleActorRequest, matchesActorRequest } from '@sigx/actors/server';
import {
    cluster,
    matchesSiloRequest,
    memoryClusterHub,
    type ClusterPlacement,
    type MemoryClusterHub,
    type PlacementPolicy
} from '@sigx/actors/cluster';

/** Deterministic silo defaults — no background churn, no call deadlines. */
export const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** Deterministic placement: every silo activates its own new actors. */
export const selfPolicy: PlacementPolicy = {
    choose: (_ref, _view, self) => self
};

/**
 * Real fetch cancels the response body when its signal aborts; an in-memory
 * handler call has no such link. Recreate it so consumer break/return()
 * reaches the server generator exactly like production.
 */
export function abortLinked(response: Response, signal: AbortSignal | null | undefined): Response {
    if (!response.body || !signal) return response;
    const reader = response.body.getReader();
    const stream = new ReadableStream<Uint8Array>({
        async pull(controller) {
            const { value, done } = await reader.read();
            if (done) controller.close();
            else controller.enqueue(value);
        },
        cancel(reason) {
            return reader.cancel(reason);
        }
    });
    signal.addEventListener('abort', () => void reader.cancel().catch(() => {}));
    return new Response(stream, { status: response.status, headers: response.headers });
}

export interface ClusterOptions {
    actors: readonly AnyActorDefinition[];
    storage?: ActorStorage;
    defaults?: SiloDefaults;
    policy?: PlacementPolicy;
    typePolicies?: Record<string, PlacementPolicy>;
    secret?: string;
    retries?: number;
    retryBackoffMs?: number;
    /** Spy hook: called with every URL crossing the pipe. */
    onRequest?: (url: string) => void;
    /** Extra plugins for silo `i` — e.g. `health()`, `metrics()`. */
    plugins?: (index: number) => readonly ActorPlugin[];
    /** Wrap the shared pipe: stall a peer, rewrite a response, count
     *  concurrency. Applied around `pipeFetch`, so it sees every hop. */
    wrapFetch?: (inner: typeof globalThis.fetch) => typeof globalThis.fetch;
}

export interface ClusterHarness {
    silos: Silo[];
    /** The apps behind the silos — for probing contributed routes. */
    apps: ActorApp[];
    placements: ClusterPlacement[];
    hub: MemoryClusterHub;
    storage: ActorStorage;
    /** The shared pipe — pass as `fetch` to `configureActors`. */
    fetch: typeof globalThis.fetch;
    /** Public actor endpoint of silo `i` (for `configureActors`). */
    endpointOf(i: number): string;
    /** Simulate a crash of silo `i`: membership drops it AND its address
     *  stops resolving (fetch → unreachable). No cleanup runs. */
    crash(i: number): void;
    /** Simulate a network partition: silo `i`'s address stops resolving
     *  but its membership heartbeat stays alive. */
    unbind(i: number): void;
    stop(): Promise<void>;
}

export async function createCluster(n: number, options: ClusterOptions): Promise<ClusterHarness> {
    const hub = memoryClusterHub();
    const storage = options.storage ?? memoryStorage();
    const secret = options.secret ?? 'test-secret';
    const registry = new Map<string, { app: ActorApp; silo: Silo }>();

    const rawFetch: typeof globalThis.fetch = async (input, init) => {
        const request = new Request(input, init);
        const url = new URL(request.url);
        options.onRequest?.(request.url);
        const member = registry.get(url.host);
        // No listener at that address — exactly a connection refusal, which
        // the transport classifies as unreachable.
        if (!member) throw new TypeError(`fetch failed: connection refused to ${url.host}`);
        // The internal mount arrives as a PLUGIN ROUTE now, so the whole
        // existing suite exercises the cluster() wiring end to end.
        const route = member.app.routes.find((candidate) => candidate.match(request));
        const response = route
            ? await route.handle(request, member.silo)
            : await handleActorRequest(request, { silo: member.silo, origin: false });
        expect(matchesSiloRequest(request) || matchesActorRequest(request)).toBe(true);
        return abortLinked(response, init?.signal ?? request.signal);
    };
    const pipeFetch = options.wrapFetch ? options.wrapFetch(rawFetch) : rawFetch;

    const silos: Silo[] = [];
    const placements: ClusterPlacement[] = [];
    const apps: ActorApp[] = [];
    for (let i = 0; i < n; i++) {
        const plugin = cluster({
            providers: hub.providers(),
            advertise: `http://silo${i}.test`,
            secret,
            fetch: pipeFetch,
            ...(options.policy ? { policy: options.policy } : {}),
            ...(options.typePolicies ? { typePolicies: options.typePolicies } : {}),
            ...(options.retries !== undefined ? { retries: options.retries } : {}),
            ...(options.retryBackoffMs !== undefined
                ? { retryBackoffMs: options.retryBackoffMs }
                : {})
        });
        let app = defineActorApp({
            actors: options.actors,
            storage,
            defaults: { ...quiet, ...options.defaults }
        }).use(plugin);
        for (const extra of options.plugins?.(i) ?? []) app = app.use(extra);
        const silo = await app.start();
        registry.set(`silo${i}.test`, { app, silo });
        silos.push(silo);
        placements.push(plugin.placement);
        apps.push(app);
    }

    return {
        silos,
        apps,
        placements,
        hub,
        storage,
        fetch: pipeFetch,
        endpointOf: (i) => `http://silo${i}.test/_sigx/actor`,
        crash: (i) => {
            registry.delete(`silo${i}.test`);
            hub.kill(placements[i]!.identity.siloId);
        },
        unbind: (i) => {
            registry.delete(`silo${i}.test`);
        },
        stop: async () => {
            await Promise.allSettled(apps.map((a) => a.stop({ timeoutMs: 1000 })));
        }
    };
}
