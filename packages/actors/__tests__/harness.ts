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
import { createSilo, memoryStorage, type SiloDefaults } from '@sigx/actors/silo';
import { handleActorRequest, matchesActorRequest } from '@sigx/actors/server';
import {
    clusterPlacement,
    handleSiloRequest,
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
    secret?: string;
    retries?: number;
    retryBackoffMs?: number;
    /** Spy hook: called with every URL crossing the pipe. */
    onRequest?: (url: string) => void;
}

export interface ClusterHarness {
    silos: Silo[];
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
    const registry = new Map<string, { silo: Silo; placement: ClusterPlacement }>();

    const pipeFetch: typeof globalThis.fetch = async (input, init) => {
        const request = new Request(input, init);
        const url = new URL(request.url);
        options.onRequest?.(request.url);
        const member = registry.get(url.host);
        // No listener at that address — exactly a connection refusal, which
        // the transport classifies as unreachable.
        if (!member) throw new TypeError(`fetch failed: connection refused to ${url.host}`);
        const response = matchesSiloRequest(request)
            ? await handleSiloRequest(request, {
                  silo: member.silo,
                  placement: member.placement,
                  secret
              })
            : await handleActorRequest(request, { silo: member.silo, origin: false });
        expect(matchesSiloRequest(request) || matchesActorRequest(request)).toBe(true);
        return abortLinked(response, init?.signal ?? request.signal);
    };

    const silos: Silo[] = [];
    const placements: ClusterPlacement[] = [];
    for (let i = 0; i < n; i++) {
        const placement = clusterPlacement({
            ...hub.providers(),
            advertise: `http://silo${i}.test`,
            secret,
            fetch: pipeFetch,
            ...(options.policy ? { policy: options.policy } : {}),
            ...(options.retries !== undefined ? { retries: options.retries } : {}),
            ...(options.retryBackoffMs !== undefined
                ? { retryBackoffMs: options.retryBackoffMs }
                : {})
        });
        const silo = createSilo({
            actors: options.actors,
            storage,
            placement,
            defaults: { ...quiet, ...options.defaults }
        });
        registry.set(`silo${i}.test`, { silo, placement });
        silos.push(silo);
        placements.push(placement);
        await silo.start();
    }

    return {
        silos,
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
            await Promise.allSettled(silos.map((s) => s.stop({ timeoutMs: 1000 })));
        }
    };
}
