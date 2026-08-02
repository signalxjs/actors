/**
 * Pull-based cluster counters — the routing-layer half of the observability
 * story `metrics()` started. Same posture: counters you READ, no exporter,
 * no push pipeline, no dependency.
 *
 * ONE rule makes these safe to sum across hosts: every counter is
 * incremented on exactly the one host where its event happened, and the two
 * sides of a cross-host event get DIFFERENT names. `remoteDispatches` (the
 * caller sent a hop) and `inboundDispatches` (the owner served one) are
 * reported side by side and never added together — their gap is in-flight,
 * refused and retried attempts, which is itself the signal.
 *
 * There is no enable/disable toggle, unlike `metrics()`. These are integer
 * increments on paths already doing async directory and network work; the
 * local fast path (`dispatcherFor` handing back the local dispatcher for a
 * claimed actor) is not instrumented at all and stays byte for byte what it
 * was.
 */
import type { HostStatus } from './types';

/** The additive fields — every one a monotonic count of events on THIS host. */
export interface ClusterCounterTotals {
    // --- routing, origin side (this host initiated the call) ---------------
    /** Routed dispatches that resolved to this host. */
    routedLocal: number;
    /** Hops sent OUT to a peer, per ATTEMPT — so retries are included. */
    remoteDispatches: number;
    remoteStreams: number;
    /**
     * Watches opened ON a peer, per ATTEMPT. Counted apart from
     * `remoteStreams` because they cost differently: a stream ends, while a
     * watch holds a keep-alive on the owner until the subscriber leaves —
     * so this is the number worth reading next to a host's activation count.
     */
    remoteWatches: number;
    /** Attempts beyond the first, whatever the cause. */
    retries: number;
    /** Calls that exhausted every attempt and threw `ActorActivationError`. */
    routingFailures: number;

    // --- routing, inbound side (this host served a peer) ------------------
    /** Calls received from a peer on the internal mount. */
    inboundDispatches: number;
    inboundStreams: number;
    /** Watches a peer opened on an actor THIS host owns. */
    inboundWatches: number;

    // --- route cache ------------------------------------------------------
    routeCacheHits: number;
    routeCacheMisses: number;

    // --- locate (the public mount's redirect decision) --------------------
    /** `locate()` calls — a mount asking WHERE without dispatching. */
    locates: number;
    /**
     * …of which answered "a peer owns it". Read against `locates` this is
     * the miss rate the edge is producing: if it stays high, whatever is
     * routing in front of the cluster is not agreeing with placement.
     */
    locateRemote: number;

    // --- directory --------------------------------------------------------
    directoryLookups: number;
    directoryClaims: number;
    /** Claims LOST to a peer — the wrong-host this host threw at a caller. */
    claimConflicts: number;
    directoryReleases: number;
    directoryEvictions: number;
    /** Departed peers whose entries this host swept. */
    hostSweeps: number;
    /** Entries removed by those sweeps. */
    sweptEntries: number;

    // --- failure classification (the `#noteFailure` funnel) ---------------
    /** 421s received: we called the wrong owner and re-routed. */
    wrongHostRedirects: number;
    unreachableRetries: number;
    /** Peer was draining (503 host-shutdown) — a rolling deploy in progress. */
    drainingRetries: number;
    /** Requests the internal mount refused as unauthenticated (403). */
    authFailures: number;
    /**
     * Peers reached by a LATER transport in the chain than the first —
     * i.e. the preferred one published no address for them. Counted
     * because a fallback is otherwise invisible: a half-rolled-out
     * transport looks exactly like a working one until you read this.
     */
    transportFallbacks: number;

    // --- membership -------------------------------------------------------
    membershipChanges: number;
    selfFences: number;

    // --- rebalancing ------------------------------------------------------
    /** `rebalance()` rounds attempted on this host (everything past the
     *  not-active guard, including rounds with no answering peer — so a
     *  partitioned host's cadence stays visible). */
    rebalanceRounds: number;
    /** Activations this host shed via `migrate()` in rebalance rounds. */
    rebalanceMigrations: number;

    // --- gauges that still sum meaningfully -------------------------------
    /** Actors currently claimed by (and therefore active on) this host. */
    claimed: number;
    /** Entries currently in the route cache (bounded at 10k). */
    routeCacheSize: number;
}

/** One host's counters: the additive fields plus its own point-in-time state. */
export interface ClusterCounters extends ClusterCounterTotals {
    /** `membership.view().version` AS THIS HOST SEES IT. A spread across
     *  hosts means the view has not converged, which explains any
     *  disagreement in the rest of the report. */
    membershipVersion: number;
    /** Wire status, plus `'fenced'`: membership lost and self-fenced.
     *  `#fence()` deliberately leaves the published status alone, so this is
     *  the only place the distinction surfaces. */
    status: HostStatus | 'fenced';
}

export function createCounters(): ClusterCounterTotals {
    return {
        routedLocal: 0,
        remoteDispatches: 0,
        remoteStreams: 0,
        remoteWatches: 0,
        retries: 0,
        routingFailures: 0,
        inboundDispatches: 0,
        inboundStreams: 0,
        inboundWatches: 0,
        routeCacheHits: 0,
        routeCacheMisses: 0,
        locates: 0,
        locateRemote: 0,
        directoryLookups: 0,
        directoryClaims: 0,
        claimConflicts: 0,
        directoryReleases: 0,
        directoryEvictions: 0,
        hostSweeps: 0,
        sweptEntries: 0,
        wrongHostRedirects: 0,
        unreachableRetries: 0,
        drainingRetries: 0,
        authFailures: 0,
        transportFallbacks: 0,
        membershipChanges: 0,
        selfFences: 0,
        rebalanceRounds: 0,
        rebalanceMigrations: 0,
        claimed: 0,
        routeCacheSize: 0
    };
}

/**
 * Sum two counter sets field-wise. Only the additive fields — `status` and
 * `membershipVersion` are per-host facts that a total would destroy, which
 * is why they are not on `ClusterCounterTotals`.
 */
export function addCounters(
    a: ClusterCounterTotals,
    b: ClusterCounterTotals
): ClusterCounterTotals {
    const out = createCounters();
    for (const key of Object.keys(out) as (keyof ClusterCounterTotals)[]) {
        out[key] = a[key] + b[key];
    }
    return out;
}
