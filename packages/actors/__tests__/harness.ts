/**
 * Shared test harness: an N-host in-process cluster with zero sockets.
 * Every host gets a fake origin (`http://host<i>.test`); the shared
 * `pipeFetch` routes host-to-host and public wire requests straight into
 * the target host's real handlers, re-creating fetch's body-cancel-on-abort
 * link so stream cancellation behaves like production.
 *
 * HAZARD: `host.start()` stamps the LAST-WINS `__SIGX_ACTOR_HOST__` global,
 * so multi-host tests must always dispatch through `host.actor()` /
 * `configureActors({ fetch })` pipes — never the ambient `actor()` on the
 * server path.
 */
import { expect } from 'vitest';
import type { ActorStorage, AnyActorDefinition, Host } from '@sigx/actors';
import {
    defineActorApp,
    memoryStorage,
    type ActorApp,
    type ActorPlugin,
    type HostDefaults
} from '@sigx/actors/host';
import {
    handleActorRequest,
    matchesActorRequest,
    type ActorMissPolicy
} from '@sigx/actors/server';
import {
    cluster,
    matchesHostRequest,
    memoryClusterHub,
    type ClusterPlacement,
    type ClusterPluginOptions,
    type MemoryClusterHub,
    type PlacementPolicy
} from '@sigx/actors/cluster';

/** Deterministic host defaults — no background churn, no call deadlines. */
export const quiet = { sweepIntervalMs: 60_000, reminderTickMs: 60_000, callTimeoutMs: 0 };

/** Deterministic placement: every host activates its own new actors. */
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
    defaults?: HostDefaults;
    policy?: PlacementPolicy;
    typePolicies?: Record<string, PlacementPolicy>;
    /** `null` builds an UNAUTHENTICATED cluster; omitted uses the default. */
    secret?: string | null;
    retries?: number;
    retryBackoffMs?: number;
    /** Spy hook: called with every URL crossing the pipe. */
    onRequest?: (url: string) => void;
    /** Miss policy for the PUBLIC actor mount. Default `'proxy'`. */
    onMiss?: ActorMissPolicy;
    /** Forwarded to `cluster({ rebalance })` on every host. */
    rebalance?: ClusterPluginOptions['rebalance'];
    /** Client-reachable origin of host `i`. The pipe already resolves
     *  `host<i>.test`, so a redirect is genuinely followable in-process. */
    publicAddress?: (index: number) => string;
    /** Extra plugins for host `i` — e.g. `health()`, `metrics()`. */
    plugins?: (index: number) => readonly ActorPlugin[];
    /** Wrap the shared pipe: stall a peer, rewrite a response, count
     *  concurrency. Applied around `pipeFetch`, so it sees every hop. */
    wrapFetch?: (inner: typeof globalThis.fetch) => typeof globalThis.fetch;
}

export interface ClusterHarness {
    hosts: Host[];
    /** The apps behind the hosts — for probing contributed routes. */
    apps: ActorApp[];
    placements: ClusterPlacement[];
    hub: MemoryClusterHub;
    storage: ActorStorage;
    /** The shared pipe — pass as `fetch` to `configureActors`. */
    fetch: typeof globalThis.fetch;
    /** Public actor endpoint of host `i` (for `configureActors`). */
    endpointOf(i: number): string;
    /** Simulate a crash of host `i`: membership drops it AND its address
     *  stops resolving (fetch → unreachable). No cleanup runs. */
    crash(i: number): void;
    /** Simulate a network partition: host `i`'s address stops resolving
     *  but its membership heartbeat stays alive. */
    unbind(i: number): void;
    stop(): Promise<void>;
}

export async function createCluster(n: number, options: ClusterOptions): Promise<ClusterHarness> {
    const hub = memoryClusterHub();
    const storage = options.storage ?? memoryStorage();
    const secret = options.secret === null ? undefined : (options.secret ?? 'test-secret');
    const registry = new Map<string, { app: ActorApp; host: Host }>();

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
            ? await route.handle(request, member.host)
            : await handleActorRequest(request, {
                  host: member.host,
                  origin: false,
                  ...(options.onMiss !== undefined ? { onMiss: options.onMiss } : {})
              });
        expect(matchesHostRequest(request) || matchesActorRequest(request)).toBe(true);
        return abortLinked(response, init?.signal ?? request.signal);
    };
    const pipeFetch = options.wrapFetch ? options.wrapFetch(rawFetch) : rawFetch;

    const hosts: Host[] = [];
    const placements: ClusterPlacement[] = [];
    const apps: ActorApp[] = [];
    for (let i = 0; i < n; i++) {
        const plugin = cluster({
            providers: hub.providers(),
            advertise: `http://host${i}.test`,
            ...(options.publicAddress ? { publicAddress: options.publicAddress(i) } : {}),
            ...(secret !== undefined ? { secret } : {}),
            fetch: pipeFetch,
            ...(options.policy ? { policy: options.policy } : {}),
            ...(options.typePolicies ? { typePolicies: options.typePolicies } : {}),
            ...(options.retries !== undefined ? { retries: options.retries } : {}),
            ...(options.retryBackoffMs !== undefined
                ? { retryBackoffMs: options.retryBackoffMs }
                : {}),
            ...(options.rebalance ? { rebalance: options.rebalance } : {})
        });
        let app = defineActorApp({
            actors: options.actors,
            storage,
            defaults: { ...quiet, ...options.defaults }
        }).use(plugin);
        for (const extra of options.plugins?.(i) ?? []) app = app.use(extra);
        const host = await app.start();
        registry.set(`host${i}.test`, { app, host });
        hosts.push(host);
        placements.push(plugin.placement);
        apps.push(app);
    }

    return {
        hosts,
        apps,
        placements,
        hub,
        storage,
        fetch: pipeFetch,
        endpointOf: (i) => `http://host${i}.test/_sigx/actor`,
        crash: (i) => {
            registry.delete(`host${i}.test`);
            hub.kill(placements[i]!.identity.hostId);
        },
        unbind: (i) => {
            registry.delete(`host${i}.test`);
        },
        stop: async () => {
            await Promise.allSettled(apps.map((a) => a.stop({ timeoutMs: 1000 })));
        }
    };
}
