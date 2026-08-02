/**
 * `activationCountPolicy()` — steer NEW activations toward the least-loaded
 * host, the production default posture for uneven workloads.
 *
 * Three ideas, each load-bearing:
 *
 * - **A cached load view, refreshed out of band.** `choose()` sits on the
 *   dispatch hot path and must stay sync, so it only ever reads a map. The
 *   map is refreshed on a cadence over the authenticated host-to-host ops
 *   channel (`PolicyRuntime.peerLoad`, one round trip per peer) — never per
 *   decision. Staleness is a feature, not a bug to engineer away: routing
 *   is an optimization, and a decision made on old numbers costs a little
 *   balance, never correctness.
 * - **Power-of-two-choices, not global-min.** Between refreshes every host
 *   would send its whole burst to the same "coldest" peer; sampling two
 *   candidates and taking the less loaded spreads that herd with near-min
 *   quality (the classic balls-into-bins result). Attached but not yet
 *   refreshed, ties resolve to the first uniform sample and the pending
 *   delta spreads the interim — random-or-better until data lands.
 * - **A local pending delta.** Placements this host made since the last
 *   refresh are counted on top of the fetched numbers, so a burst arriving
 *   inside one refresh window sees its own effect. Cleared whenever fresh
 *   truth arrives.
 *
 * A host with NO known load reads as 0 — cold until proven busy — which is
 * what makes a freshly joined host attract work immediately.
 *
 * Un-attached (single node, foreign backend, or a placement that never
 * starts), `choose()` returns its first uniform sample and keeps no state
 * at all — exactly `randomPlacementPolicy()`, with nothing accumulating.
 */
import { CLUSTER_BACKEND } from './placement';
import type { HostDescriptor, PlacementPolicy, PolicyRuntime } from './types';

export interface ActivationCountPolicyOptions {
    /**
     * Load-view refresh cadence, ms. Default 5000. The staleness bound:
     * between refreshes the pending delta is the only signal.
     */
    refreshMs?: number;
    /** Per-peer probe budget, ms. Default 1000. A peer that misses it
     *  keeps its stale entry. */
    timeoutMs?: number;
    /** Concurrent probes per refresh. Default 8. */
    concurrency?: number;
}

export function activationCountPolicy(
    options: ActivationCountPolicyOptions = {}
): PlacementPolicy {
    const refreshMs = options.refreshMs ?? 5_000;
    const timeoutMs = options.timeoutMs ?? 1_000;
    const concurrency = Math.max(1, options.concurrency ?? 8);

    /** hostId → last fetched activation count. Cluster-wide truth, so one
     *  instance may safely serve several in-process placements (tests). */
    const loads = new Map<string, number>();
    /** hostId → placements chosen here since the last refresh. */
    const pending = new Map<string, number>();
    const runtimes = new Set<PolicyRuntime>();

    const loadOf = (hostId: string): number =>
        (loads.get(hostId) ?? 0) + (pending.get(hostId) ?? 0);

    /** One refresh in flight at a time: a slow round must not stack probes
     *  behind itself — the next tick simply finds the flag set and skips. */
    let inFlight = false;

    const refresh = async (runtime: PolicyRuntime): Promise<void> => {
        if (inFlight) return;
        inFlight = true;
        try {
            // Re-checked around every write: a teardown mid-round must not
            // let the tail of this refresh repopulate the cleared maps.
            const live = (): boolean => runtimes.has(runtime);
            const view = runtime.view();
            const peers = view.hosts.filter(
                (h) => h.status === 'active' && h.hostId !== runtime.hostId
            );
            if (!live()) return;
            loads.set(runtime.hostId, runtime.selfLoad());
            let next = 0;
            const worker = async (): Promise<void> => {
                while (next < peers.length) {
                    const peer = peers[next++]!;
                    try {
                        const load = await runtime.peerLoad(peer, timeoutMs);
                        if (!live()) return;
                        loads.set(peer.hostId, load);
                    } catch {
                        // Keep the stale value — never act on missing data.
                        // An unreachable peer's entry ages until the view
                        // drops it.
                    }
                }
            };
            await Promise.all(
                Array.from({ length: Math.min(concurrency, peers.length) }, worker)
            );
            if (!live()) return;
            // Departed hosts leave the map with the view; fresh truth resets
            // the predictions.
            const known = new Set(runtime.view().hosts.map((h) => h.hostId));
            for (const id of [...loads.keys()]) if (!known.has(id)) loads.delete(id);
            pending.clear();
        } finally {
            inFlight = false;
        }
    };

    return {
        name: 'activation-count',
        backend: CLUSTER_BACKEND,
        attach(runtime) {
            runtimes.add(runtime);
            void refresh(runtime);
            const timer = setInterval(() => void refresh(runtime), refreshMs);
            // A load refresher must never hold a process open.
            (timer as { unref?: () => void }).unref?.();
            return () => {
                clearInterval(timer);
                runtimes.delete(runtime);
                if (runtimes.size === 0) {
                    loads.clear();
                    pending.clear();
                }
            };
        },
        choose(_ref, view, self): HostDescriptor {
            const active = view.hosts.filter((h) => h.status === 'active');
            if (active.length === 0) return self;
            if (active.length === 1) return active[0]!;
            // Two distinct uniform samples; the classic j >= i shift keeps
            // the second draw uniform over the remaining hosts.
            const i = Math.floor(Math.random() * active.length);
            let j = Math.floor(Math.random() * (active.length - 1));
            if (j >= i) j += 1;
            const a = active[i]!;
            // Un-attached there is no refresh to ever clear the pending map,
            // so no bookkeeping happens at all — the first sample IS a
            // uniform draw, which is what makes the random-equivalence claim
            // exact rather than approximate.
            if (runtimes.size === 0) return a;
            const b = active[j]!;
            const chosen = loadOf(b.hostId) < loadOf(a.hostId) ? b : a;
            pending.set(chosen.hostId, (pending.get(chosen.hostId) ?? 0) + 1);
            return chosen;
        }
    };
}
