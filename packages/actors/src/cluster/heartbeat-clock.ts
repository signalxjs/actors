/**
 * The self-suspicion clock every TTL-heartbeat membership provider shares
 * (#45).
 *
 * A heartbeat proves presence at an INSTANT; membership needs presence over
 * an INTERVAL. The record a beat writes lives for `ttlMs`, so this host is
 * only entitled to assume it is still in the cluster until
 * `lastConfirmedWrite + ttlMs`. Past that the record expired, peers dropped
 * this host from their views and `evictHost` released every directory claim
 * it held — regardless of whether anything ever failed here.
 *
 * That last clause is the bug. Providers used to suspect only from a write
 * REJECTION (`writeSelf().catch(...)`), so a host whose event loop stalled
 * past the TTL — a 20 s GC pause, a suspended container, a blocked sync call
 * — resumed, wrote late, SUCCEEDED, and kept serving actors a survivor had
 * already re-activated. Two live activations of one actor, which is the one
 * invariant the runtime exists to hold. The fix is to judge the GAP, not the
 * outcome: check before each write whether the window lapsed.
 *
 * Two clocks, because each misses a real case the other catches:
 *
 * - **Monotonic** (`performance.now()`) is immune to NTP steps and to an
 *   operator moving the wall clock, so it is what a GC pause or a blocked
 *   loop should be judged on.
 * - **Wall** (`Date.now()`) is the only one that sees a SUSPEND.
 *   `CLOCK_MONOTONIC` does not advance while a VM or container is paused,
 *   and `setTimeout` rides that same clock — so after a five-minute suspend
 *   the beat looks perfectly punctual to the monotonic clock while the
 *   store, on another machine, expired the key minutes ago.
 *
 * The lapse is therefore `max(monotonic elapsed, wall elapsed)`: either
 * clock noticing is enough, and `max` ignores a wall clock that stepped
 * BACKWARDS (a negative delta simply loses to the monotonic one) so a
 * correcting NTP step can never mask a real lapse.
 *
 * Firing is latched — one `onSuspect` per lapse, not one per beat — and a
 * confirmed write un-latches, matching what the providers documented before
 * they shared this code.
 */

/** What a provider drives on each heartbeat. */
export interface HeartbeatClock {
    /**
     * Open (or re-open) the presence window at "now" WITHOUT a write, and
     * clear the latch.
     *
     * Providers must call this immediately before arming their beat
     * interval, not just after their join write. Between the two sits the
     * rest of `join()` — `sadd`, a version bump, a full O(N) `refresh()` —
     * and on a slow or large store that chain can itself outlast `ttlMs`.
     * Stamping only at the write would then make the FIRST beat late by
     * construction: a terminal fence, a failed liveness probe and a pod
     * restart, in a loop, on every host — strictly worse than the bug this
     * clock exists to fix. Stamping here is sound because a host that has
     * not begun beating holds no claims yet, so there is nothing for a peer
     * to have evicted.
     */
    arm(): void;
    /**
     * Call at the START of every beat, before issuing the write. Fires
     * `onSuspect` (once) if the presence window has lapsed.
     *
     * Before the write, deliberately: the window is already gone by then,
     * and every millisecond spent waiting on a store round-trip is another
     * millisecond a doomed activation keeps accepting turns.
     */
    beat(): void;
    /** A heartbeat write CONFIRMED: re-open the window, clear the latch. */
    confirmed(): void;
    /** A heartbeat write FAILED: fire if the window has lapsed. */
    failed(): void;
}

export interface HeartbeatClockOptions {
    /**
     * The provider's record TTL — the same value its heartbeat writes.
     * Presence is assumed for this long after each confirmed write.
     */
    ttlMs: number;
    /** Fired at most once per lapse; a confirmed write re-arms it. */
    onSuspect: () => void;
    /** Test seams. Default to `performance.now()` / `Date.now()`. */
    monotonicNow?: () => number;
    wallNow?: () => number;
}

export function heartbeatClock(options: HeartbeatClockOptions): HeartbeatClock {
    const { ttlMs, onSuspect } = options;
    const monotonicNow = options.monotonicNow ?? ((): number => performance.now());
    const wallNow = options.wallNow ?? ((): number => Date.now());

    let armed = false;
    let mono = 0;
    let wall = 0;
    let suspected = false;

    const open = (): void => {
        armed = true;
        mono = monotonicNow();
        wall = wallNow();
        suspected = false;
    };

    // Either clock noticing is enough; a wall clock that stepped backwards
    // yields a negative delta and simply loses to the monotonic one.
    const lapsed = (): boolean =>
        armed && Math.max(monotonicNow() - mono, wallNow() - wall) > ttlMs;

    const suspect = (): void => {
        if (suspected || !lapsed()) return;
        suspected = true;
        onSuspect();
    };

    return {
        arm: open,
        beat: suspect,
        confirmed: open,
        failed: suspect
    };
}
