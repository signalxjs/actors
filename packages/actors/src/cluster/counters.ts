/**
 * Pull-based cluster counters — the routing-layer half of the observability
 * story `metrics()` started. Same posture: counters you READ, no exporter,
 * no push pipeline, no dependency.
 *
 * ONE rule makes these safe to sum across silos: every counter is
 * incremented on exactly the one silo where its event happened, and the two
 * sides of a cross-silo event get DIFFERENT names. `remoteDispatches` (the
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
import type { SiloStatus } from './types';

/** The additive fields — every one a monotonic count of events on THIS silo. */
export interface ClusterCounterTotals {
    // --- routing, origin side (this silo initiated the call) ---------------
    /** Routed dispatches that resolved to this silo. */
    routedLocal: number;
    /** Hops sent OUT to a peer, per ATTEMPT — so retries are included. */
    remoteDispatches: number;
    remoteStreams: number;
    /** Attempts beyond the first, whatever the cause. */
    retries: number;
    /** Calls that exhausted every attempt and threw `ActorActivationError`. */
    routingFailures: number;

    // --- routing, inbound side (this silo served a peer) ------------------
    /** Calls received from a peer on the internal mount. */
    inboundDispatches: number;
    inboundStreams: number;

    // --- route cache ------------------------------------------------------
    routeCacheHits: number;
    routeCacheMisses: number;

    // --- directory --------------------------------------------------------
    directoryLookups: number;
    directoryClaims: number;
    /** Claims LOST to a peer — the wrong-host this silo threw at a caller. */
    claimConflicts: number;
    directoryReleases: number;
    directoryEvictions: number;
    /** Departed peers whose entries this silo swept. */
    siloSweeps: number;
    /** Entries removed by those sweeps. */
    sweptEntries: number;

    // --- failure classification (the `#noteFailure` funnel) ---------------
    /** 421s received: we called the wrong owner and re-routed. */
    wrongHostRedirects: number;
    unreachableRetries: number;
    /** Peer was draining (503 silo-shutdown) — a rolling deploy in progress. */
    drainingRetries: number;
    /** Requests the internal mount refused as unauthenticated (403). */
    authFailures: number;

    // --- membership -------------------------------------------------------
    membershipChanges: number;
    selfFences: number;

    // --- gauges that still sum meaningfully -------------------------------
    /** Actors currently claimed by (and therefore active on) this silo. */
    claimed: number;
    /** Entries currently in the route cache (bounded at 10k). */
    routeCacheSize: number;
}

/** One silo's counters: the additive fields plus its own point-in-time state. */
export interface ClusterCounters extends ClusterCounterTotals {
    /** `membership.view().version` AS THIS SILO SEES IT. A spread across
     *  silos means the view has not converged, which explains any
     *  disagreement in the rest of the report. */
    membershipVersion: number;
    /** Wire status, plus `'fenced'`: membership lost and self-fenced.
     *  `#fence()` deliberately leaves the published status alone, so this is
     *  the only place the distinction surfaces. */
    status: SiloStatus | 'fenced';
}

export function createCounters(): ClusterCounterTotals {
    return {
        routedLocal: 0,
        remoteDispatches: 0,
        remoteStreams: 0,
        retries: 0,
        routingFailures: 0,
        inboundDispatches: 0,
        inboundStreams: 0,
        routeCacheHits: 0,
        routeCacheMisses: 0,
        directoryLookups: 0,
        directoryClaims: 0,
        claimConflicts: 0,
        directoryReleases: 0,
        directoryEvictions: 0,
        siloSweeps: 0,
        sweptEntries: 0,
        wrongHostRedirects: 0,
        unreachableRetries: 0,
        drainingRetries: 0,
        authFailures: 0,
        membershipChanges: 0,
        selfFences: 0,
        claimed: 0,
        routeCacheSize: 0
    };
}

/**
 * Sum two counter sets field-wise. Only the additive fields — `status` and
 * `membershipVersion` are per-silo facts that a total would destroy, which
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
