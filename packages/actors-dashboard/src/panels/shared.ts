/**
 * Lines more than one panel needs, and the panel prop shape.
 *
 * Every one of these formats an already-decided fact. Anything that DECIDES
 * something — what is wrong, what a number is about, whether a shard state is
 * an incident — comes from `@sigx/actors-monitor` and is not re-derived here.
 */
import { toRaw } from '@sigx/reactivity';
import { count, durationMs } from '@sigx/actors-monitor/format';
import type { DashboardState, HostView, MonitorSnapshot } from '@sigx/actors-monitor';
import type { HealthReport, HealthStatus } from '@sigx/actors/host';

/**
 * What every panel takes. One object, so they compose in any order.
 *
 * Every panel is a real `component()` rather than a plain function that
 * returns JSX. Both work as JSX in sigx, but only the first gets its own
 * reactive scope — and without that, a snapshot arriving would re-render the
 * shell around it once a second, taking the tab strip with it. Keyboard focus
 * does not survive that, which would make the tabs unusable for anyone not
 * using a mouse.
 */
export interface PanelProps {
    state: DashboardState;
}

/**
 * The `DashboardState` behind a panel's props — **unwrapped**.
 *
 * Every panel starts with this, and a panel you write yourself must too.
 * Props arrive through a reactive proxy, and `DashboardState`, `Series` and
 * `RateTracker` all hold `#private` fields: a `#`-field read resolves against
 * the receiver, so calling `state.calls.values()` on the proxy throws
 * `Cannot read private member #values from an object whose class did not
 * declare it`. It is a hard failure rather than a wrong number, which is the
 * good version of this bug, but only if you know the answer.
 *
 * Unwrapping costs no reactivity. The panels do not track the state OBJECT —
 * they track `state.view`, which is a signal in its own right, and the
 * derived `Series` are re-read whenever it changes. Unwrapping also keeps the
 * poll loop's own bookkeeping (`#inFlight`, `#timer`) out of the reactive
 * graph, where every tick would otherwise invalidate something.
 *
 * `toRaw` is identity on a value that was never proxied, so this is safe on a
 * state passed in directly.
 */
export const panelState = (props: PanelProps): DashboardState => toRaw(props.state);

/**
 * Has a poll come back yet, and if not, why not?
 *
 * Every panel needs this because every panel has nothing to draw until the
 * first snapshot lands — and the shape that reads naturally,
 *
 *     if (!snapshot) return <p>connecting…</p>;
 *
 * is a trap: it returns BEFORE the alert banner, and a failed first poll is
 * exactly the case where there is no snapshot. So the one failure a new user
 * is most likely to hit — the source cannot be reached at all — rendered
 * "connecting…" forever with the reason demoted to small text in the corner
 * (#256). A banner that goes quiet precisely when it matters is worse than no
 * banner, because the silence reads as "still working on it".
 *
 * Callers guard on `state.view.snapshot` themselves and pass this into
 * `<Awaiting>`, which renders the banner above the message. That keeps the
 * narrowing TypeScript already does — no `!` on the snapshot afterwards —
 * while putting the ordering somewhere it cannot be got wrong per panel.
 */
export function awaitingReason(state: DashboardState): { message: string; failed: boolean } {
    if (!state.view.error) return { message: 'connecting…', failed: false };
    return {
        message: `could not reach ${state.source.label} — no data has arrived yet`,
        failed: true
    };
}

/** Readiness checks, one line each — `FAIL` first so it reads at a glance. */
export function checkLines(health: HealthReport | HealthStatus | null): string[] {
    if (!health) return [];
    return Object.entries(health.checks).map(
        ([name, check]) =>
            `${check.ready ? 'ok  ' : 'FAIL'} ${name}${check.detail ? ` — ${check.detail}` : ''}`
    );
}

/**
 * Error kinds, commonest first.
 *
 * Typed structurally rather than as `MetricsDigest`, because the two places
 * this is called from carry different shapes for the same fact: a host's own
 * digest (`HostView.metrics`) and the polled host's snapshot
 * (`ActorMetricsSnapshot`). Both have `errors.byKind`, and asking for only
 * what is read is what lets one function serve both.
 */
export function errorKindLines(
    metrics: { errors: { byKind: Record<string, number | undefined> } } | null | undefined
): string[] {
    if (!metrics) return [];
    return Object.entries(metrics.errors.byKind)
        .sort((a, b) => (b[1] ?? 0) - (a[1] ?? 0))
        .map(([kind, n]) => `${kind.padEnd(20)} ${count(n ?? 0)}`);
}

/** Recent failures, newest first — a log, so it is the block that truncates. */
export function recentFailureLines(
    recent: readonly { at: number; type: string; method: string; kind: string; message: string }[],
    limit = 8
): string[] {
    return recent
        .slice(-limit)
        .reverse()
        .map(
            (entry) =>
                `${new Date(entry.at).toISOString().slice(11, 19)} ${entry.type}#${entry.method} ` +
                `${entry.kind}: ${entry.message}`
        );
}

/**
 * The `calls` row: total, failed, and the one-way failures that are only
 * worth a column when they are non-zero.
 *
 * One-way calls fail with nobody waiting on the result, so they are invisible
 * in every other number on the panel.
 */
export function callsValue(calls: {
    total: number;
    failed: number;
    oneWayFailures?: number;
}): string {
    const oneWay = calls.oneWayFailures ?? 0;
    const failedPct = calls.total > 0 ? `${Math.round((calls.failed / calls.total) * 100)}%` : '—';
    return (
        `${count(calls.total)}  ${count(calls.failed)} failed (${failedPct})` +
        (oneWay > 0 ? `  ${count(oneWay)} one-way` : '')
    );
}

export const callsTone = (calls: { failed: number; oneWayFailures?: number }): 'warn' | null =>
    calls.failed > 0 || (calls.oneWayFailures ?? 0) > 0 ? 'warn' : null;

/** Slowest methods by p99 turn — where a slow dashboard sends you first. */
export function slowestMethodLines(snapshot: MonitorSnapshot, limit = 8): string[] {
    const metrics = snapshot.metrics;
    if (!metrics) return [];
    return Object.entries(metrics.byMethod)
        .sort((a, b) => (b[1].turnMs?.p99Ms ?? 0) - (a[1].turnMs?.p99Ms ?? 0))
        .slice(0, limit)
        .map(
            ([name, m]) =>
                `${name.padEnd(30)} ${durationMs(m.turnMs?.p99Ms ?? 0).padStart(9)}  ` +
                `${count(m.calls)} calls${m.failed > 0 ? `  ${count(m.failed)} failed` : ''}`
        );
}

/** Every host's readiness as one word. `FATAL` is not "very not ready". */
export function readyWord(host: HostView): string {
    if (!host.health) return '—';
    if (host.health.fatal) return 'FATAL';
    return host.health.ready ? 'yes' : 'NO';
}
