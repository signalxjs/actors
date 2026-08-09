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
}

/** What `snapshot()` reports — the ops-section payload. */
export interface SocketStatsSnapshot extends SocketStatsTotals {
    /** Sessions currently open. */
    open: number;
    /** Calls currently in flight across open sessions. */
    inFlight: number;
    /** Live subscriptions currently held across open sessions. */
    subscriptions: number;
    /** Completed connections' lifetimes. */
    lifetimeMs: HistogramSnapshot | null;
}

/** The session's private half — how one session reports in. */
export interface SocketSessionRecorder {
    opened(session: { stats(): { inFlight: number; subscriptions: number } }): void;
    closed(
        session: { stats(): { inFlight: number; subscriptions: number } },
        openedAtMs: number
    ): void;
    refused(): void;
    callStarted(): void;
    callFailed(): void;
    subscriptionOpened(): void;
    subscriptionClosed(): void;
    protocolBreach(): void;
    lifetimeClose(): void;
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
        lifetimeCloses: 0
    };
    const open = new Set<{ stats(): { inFlight: number; subscriptions: number } }>();
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
        snapshot() {
            let inFlight = 0;
            let subscriptions = 0;
            for (const session of open) {
                const s = session.stats();
                inFlight += s.inFlight;
                subscriptions += s.subscriptions;
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
