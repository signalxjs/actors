/**
 * Observability for socket sessions (#166): one stats object per host (or
 * per listener — whoever constructs sessions owns it), recorded into by
 * every session it is handed, published by the APP as an ops section.
 *
 * The shape follows the repo's counter discipline (`cluster/counters.ts`):
 * there is deliberately no public counter-recording API on the runtime, so
 * a subsystem owns a flat all-numbers totals object, increments at the
 * event sites it already owns, and publishes from a plugin:
 *
 * ```ts
 * const stats = socketStats();
 * // hand it to every session / attachActorSocket({ stats, … })
 * registry.reportOps('sockets', () => stats.snapshot());
 * ```
 *
 * The section then rides `GET /_sigx/ops` under `ops.sockets`, behind the
 * ops endpoint's existing bearer posture, with no endpoint changes. Totals
 * are monotonic and sum across hosts; the gauges (`open`, `inFlight`,
 * `subscriptions`) are computed from the live sessions at read time.
 * Connection lifetimes feed a log-linear histogram in the runtime's own
 * layout, so the digest merges like every other histogram.
 */
import { Histogram, HISTOGRAM_LAYOUT } from '../host/histogram';
import type { HistogramDigest, HistogramSnapshot } from '../host/histogram';

/** Monotonic totals. Flat and all-numbers so hosts sum field-wise. */
export interface SocketStatsTotals {
    /** Sessions that completed the upgrade prelude. */
    connectionsOpened: number;
    /** Sessions torn down (adapter close, protocol breach, lifetime). */
    connectionsClosed: number;
    /** Upgrades the session refused before serving a byte (origin, auth). */
    connectionsRefused: number;
    /** Calls dispatched (unary and stream alike, one-way included). */
    callsStarted: number;
    /** Calls that answered an error frame (or failed one-way). */
    callsFailed: number;
    /** Live subscriptions opened. */
    subscriptionsOpened: number;
    /** Live subscriptions closed — `{i,uns}`, terminal error, or teardown. */
    subscriptionsClosed: number;
    /** Connections closed for speaking outside the vocabulary (1003/1009). */
    protocolBreaches: number;
    /** Connections closed by `revalidateMs`/`maxConnectionMs` (1008). */
    lifetimeCloses: number;
    /**
     * `{i,v}` frames pushed for a SUBSCRIPTION — not call results, not
     * stream chunks.
     *
     * The closest thing the host has to a syscall counter: #245 profiled a
     * delivery at 77% socket write, one `writev` per subscriber, so this is
     * what a fan-out host's cost is proportional to. It is also the
     * denominator every per-delivery figure in the Tier-3 sections is
     * computed from — until now, only ever client-side, by the load
     * generator.
     */
    deliveries: number;
    /**
     * Outbound bytes for those frames, as UTF-16 code units.
     *
     * **An approximation, deliberately.** Measuring the encoded length would
     * mean a UTF-8 pass per frame on the exact path this work exists to keep
     * cheap, to sharpen a number nobody bills on. ASCII payloads are exact;
     * anything else under-reports.
     */
    deliveryBytes: number;
    /**
     * Subscriptions whose requested delivery window the policy moved (#247).
     *
     * Clients asking for something they are not getting is invisible
     * otherwise — the subscription succeeds either way — and "why is my
     * 300 ms tile updating every second?" is the support question this
     * answers.
     */
    throttleQuantized: number;
    /**
     * Subscriptions the session closed because the connection's send
     * buffer was over `maxBufferedBytes` (#258). Each is also counted in
     * `subscriptionsClosed`; this says WHY. Stays 0 with the cap off, and
     * on any adapter that cannot report its buffer — which is "the cap is
     * inert here", not "nobody was slow".
     */
    subscriptionsShed: number;
}

/** What `snapshot()` reports — the ops-section payload. */
export interface SocketStatsSnapshot extends SocketStatsTotals {
    /** Sessions currently open. */
    open: number;
    /** Calls currently in flight across open sessions. */
    inFlight: number;
    /** Live subscriptions currently held across open sessions. */
    subscriptions: number;
    /**
     * Bytes queued in the HOSTS' own send buffers, summed across open
     * sessions — `null` when no open session can report one.
     *
     * The number #182 has been arguing about without. The load generators
     * sample the CLIENT's buffer, which is a different quantity: a zero
     * there is equally consistent with the host buffering without bound,
     * which is why #208 says #182 was "left unrefuted, not answered".
     *
     * Two caveats it must be read with. It is POLLED, so it under-reports
     * peaks the same way `open` does — a burst between two `snapshot()`
     * calls is invisible. And it is only as good as the adapter: a session
     * constructed without a `bufferedBytes` callback contributes nothing,
     * so `null` means "nobody could tell us" and never "zero".
     */
    bufferedBytes: number | null;
    /** Completed connections' lifetimes. */
    lifetimeMs: HistogramSnapshot | null;
}

/**
 * What `snapshot()` polls an open session for. Structural rather than
 * `ActorSocketSession`, so a session type and its recorder do not have to
 * import each other — and it is exactly `ActorSocketSession.stats()`.
 */
export interface SocketSessionProbe {
    stats(): { inFlight: number; subscriptions: number; bufferedBytes: number | null };
}

/** The session's private half — how one session reports in. */
export interface SocketSessionRecorder {
    opened(session: SocketSessionProbe): void;
    closed(
        session: SocketSessionProbe,
        openedAtMs: number
    ): void;
    refused(): void;
    callStarted(): void;
    callFailed(): void;
    subscriptionOpened(): void;
    subscriptionClosed(): void;
    protocolBreach(): void;
    lifetimeClose(): void;
    /** One `{i,v}` subscription frame went out, carrying `chars` code units. */
    delivered(chars: number): void;
    /** A subscription's requested delivery window was moved by the policy. */
    throttleQuantized(): void;
    /** A subscription was closed for the connection's send buffer being over the cap. */
    shed(): void;
}

export interface SocketStats extends SocketSessionRecorder {
    /** The ops-section payload; cheap and synchronous by contract. */
    snapshot(): SocketStatsSnapshot;
    /** The mergeable form for `registry.reportDigest('sockets', …)`. */
    digest(): SocketStatsTotals & {
        layout: string;
        lifetime: HistogramDigest | null;
    };
}

export function socketStats(): SocketStats {
    const totals: SocketStatsTotals = {
        connectionsOpened: 0,
        connectionsClosed: 0,
        connectionsRefused: 0,
        callsStarted: 0,
        callsFailed: 0,
        subscriptionsOpened: 0,
        subscriptionsClosed: 0,
        protocolBreaches: 0,
        lifetimeCloses: 0,
        deliveries: 0,
        deliveryBytes: 0,
        throttleQuantized: 0,
        subscriptionsShed: 0
    };
    const open = new Set<SocketSessionProbe>();
    const lifetime = new Histogram();

    return {
        opened(session) {
            totals.connectionsOpened++;
            open.add(session);
        },
        closed(session, openedAtMs) {
            if (!open.delete(session)) return; // idempotent close
            totals.connectionsClosed++;
            lifetime.record(performance.now() - openedAtMs);
        },
        refused() {
            totals.connectionsRefused++;
        },
        callStarted() {
            totals.callsStarted++;
        },
        callFailed() {
            totals.callsFailed++;
        },
        subscriptionOpened() {
            totals.subscriptionsOpened++;
        },
        subscriptionClosed() {
            totals.subscriptionsClosed++;
        },
        protocolBreach() {
            totals.protocolBreaches++;
        },
        lifetimeClose() {
            totals.lifetimeCloses++;
        },
        delivered(chars) {
            totals.deliveries++;
            totals.deliveryBytes += chars;
        },
        throttleQuantized() {
            totals.throttleQuantized++;
        },
        shed() {
            totals.subscriptionsShed++;
        },
        snapshot() {
            let inFlight = 0;
            let subscriptions = 0;
            // Stays `null` unless at least one open session could answer —
            // summing into a 0 would report "the hosts are not buffering"
            // when the truth is "no adapter told us", which is the exact
            // misreading #208 was filed about.
            let bufferedBytes: number | null = null;
            for (const session of open) {
                const s = session.stats();
                inFlight += s.inFlight;
                subscriptions += s.subscriptions;
                if (s.bufferedBytes !== null) bufferedBytes = (bufferedBytes ?? 0) + s.bufferedBytes;
            }
            // `null` when nothing completed yet — nullability MEANS "no
            // data", so the runtime must not hand out an all-zeros shape a
            // renderer would read as a measurement.
            const lifetimeMs = totals.connectionsClosed > 0 ? lifetime.snapshot() : null;
            return {
                ...totals,
                open: open.size,
                inFlight,
                subscriptions,
                bufferedBytes,
                lifetimeMs
            };
        },
        digest() {
            return {
                ...totals,
                layout: HISTOGRAM_LAYOUT,
                lifetime: totals.connectionsClosed > 0 ? lifetime.digest() : null
            };
        }
    };
}
