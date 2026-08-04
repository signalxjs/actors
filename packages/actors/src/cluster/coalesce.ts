/**
 * Refresh coalescing for membership subscribers (#26).
 *
 * Every store-backed membership provider had the same idiom: a push
 * notification arrives → run a full O(N) member-list refresh. N changes
 * arriving in a burst (a rolling restart) therefore cost N refreshes PER
 * SUBSCRIBER — the measured O(N²): at n=100, one join was ~10 400 Redis
 * commands cluster-wide, and a 100-host rolling restart ~2M.
 *
 * The coalescer makes a burst cost what one change costs:
 *
 * - **Leading edge is immediate.** The first hint while idle refreshes at
 *   once — the common single change gains zero latency.
 * - **Single flight.** Hints arriving while a refresh is in flight collapse
 *   into at most ONE trailing refresh after it completes.
 * - **Version gate.** A hint carrying the store's change version is dropped
 *   outright when the local view has already caught up past it (the burst's
 *   stragglers cost zero commands). Versionless hints are never skipped.
 * - **`demand()` keeps `ClusterMembership.refresh()`'s contract**: it
 *   resolves with the result of a refresh that STARTED at-or-after the call
 *   — never a stale in-flight one. Placement's failure path leans on this
 *   ("the refreshed view excludes the leaver"), so it is load-bearing.
 *
 * `quietMs` (default 0) optionally widens the coalescing net past one
 * round-trip: after a flight completes with hints pending, wait that long
 * before the trailing refresh. The trade is ≤ quietMs of extra staleness —
 * negligible against a provider's poll interval and TTL — but 0 already
 * collapses same-RTT bursts with zero timers, so it is an operator knob,
 * not a default. `demand()` never waits out the quiet window.
 */

export interface RefreshCoalescer<V> {
    /**
     * Push-notification hint. Immediate refresh if idle; coalesced into one
     * trailing refresh if one is in flight. `version` (when the transport
     * carries it) lets an already-seen change be dropped outright.
     */
    note(version?: number): void;
    /**
     * `ClusterMembership.refresh()` semantics: resolves (or rejects) with a
     * refresh that started at-or-after this call — joins the trailing run
     * if one is due, else starts one now, cutting any quiet delay short.
     */
    demand(): Promise<V>;
    /** Resolves once no refresh is in flight or pending — for `leave()`/tests. */
    settled(): Promise<void>;
}

export interface RefreshCoalescerOptions<V> {
    /** The provider's raw full refresh. */
    refresh: () => Promise<V>;
    /** The view's current change version — the `note(version)` skip gate. */
    version: () => number;
    /** Trailing quiet window; 0 (default) is pure single-flight, no timers. */
    quietMs?: number;
    /** Test seams. */
    setTimeoutImpl?: typeof setTimeout;
    clearTimeoutImpl?: typeof clearTimeout;
}

interface Trailing<V> {
    versionless: boolean;
    /** Highest versioned hint pending; the trailing run is skippable when
     *  the view caught up past it and nobody demanded. */
    maxVersion: number;
    waiters: Array<{ resolve: (value: V) => void; reject: (reason: unknown) => void }>;
}

export function refreshCoalescer<V>(options: RefreshCoalescerOptions<V>): RefreshCoalescer<V> {
    const { refresh, version, quietMs = 0 } = options;
    const setTimeoutImpl = options.setTimeoutImpl ?? setTimeout;
    const clearTimeoutImpl = options.clearTimeoutImpl ?? clearTimeout;

    let inFlight: Promise<V> | null = null;
    /** Non-null only while a flight is up or a quiet timer is pending. */
    let trailing: Trailing<V> | null = null;
    let quiet: ReturnType<typeof setTimeout> | null = null;
    let settleWaiters: Array<() => void> = [];

    const notifySettled = (): void => {
        if (inFlight === null && trailing === null && settleWaiters.length > 0) {
            const waiters = settleWaiters;
            settleWaiters = [];
            for (const waiter of waiters) waiter();
        }
    };

    const start = (): Promise<V> => {
        let flight: Promise<V>;
        try {
            flight = refresh();
        } catch (error) {
            flight = Promise.reject(error);
        }
        inFlight = flight;
        // Settlement handling doubles as the rejection handler, so a
        // hint-triggered flight nobody awaits never surfaces as unhandled;
        // demand() callers still see the rejection through `flight` itself.
        flight.then(onFlightSettled, onFlightSettled);
        return flight;
    };

    const onFlightSettled = (): void => {
        inFlight = null;
        if (trailing === null) {
            notifySettled();
            return;
        }
        if (quietMs > 0) {
            quiet = setTimeoutImpl(runTrailing, quietMs);
            (quiet as { unref?: () => void }).unref?.();
        } else {
            runTrailing();
        }
    };

    const runTrailing = (): void => {
        quiet = null;
        const pending = trailing;
        trailing = null;
        if (pending === null) {
            notifySettled();
            return;
        }
        const due =
            pending.waiters.length > 0 ||
            pending.versionless ||
            version() < pending.maxVersion;
        if (!due) {
            notifySettled();
            return;
        }
        const flight = start();
        for (const waiter of pending.waiters) flight.then(waiter.resolve, waiter.reject);
    };

    return {
        note(noted?: number): void {
            if (noted !== undefined && noted <= version()) return;
            if (inFlight === null && trailing === null) {
                start();
                return;
            }
            trailing ??= { versionless: false, maxVersion: -Infinity, waiters: [] };
            if (noted === undefined) trailing.versionless = true;
            else if (noted > trailing.maxVersion) trailing.maxVersion = noted;
        },
        demand(): Promise<V> {
            if (inFlight === null && trailing === null) return start();
            trailing ??= { versionless: false, maxVersion: -Infinity, waiters: [] };
            const joined = new Promise<V>((resolve, reject) => {
                trailing!.waiters.push({ resolve, reject });
            });
            // Idle but quiet-delayed → the demand cuts the delay short.
            if (inFlight === null && quiet !== null) {
                clearTimeoutImpl(quiet);
                runTrailing();
            }
            return joined;
        },
        settled(): Promise<void> {
            if (inFlight === null && trailing === null) return Promise.resolve();
            return new Promise((resolve) => {
                settleWaiters.push(resolve);
            });
        }
    };
}
