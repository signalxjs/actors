/** @jsxImportSource @sigx/runtime-core */
/**
 * Overview — the panel you leave open.
 *
 * What it puts first is the design work, and it is inherited from the
 * terminal rather than re-decided: the scope label, then the four numbers
 * that describe the whole fleet, then the four series, then the three
 * histograms against ONE axis.
 *
 * Two labelling rules do the real work here, and both are #121's:
 *
 *   - cluster-wide numbers when the fan-out produced them, this host's
 *     otherwise — with the heading saying WHICH. Printing one under a label
 *     that means the other is the complaint the milestone exists for.
 *   - a rate is not a gauge. Calls and failures are per second; queue depth
 *     and activation count are how many there are right now. Labelling either
 *     as the other is the same class of lie.
 */
import {
    alertLines,
    coverageNote,
    percentileCeiling,
    percentilePoints,
    scopeOf
} from '@sigx/actors-monitor';
import { count, durationMs, gauge, rate } from '@sigx/actors-monitor/format';
import { component } from '@sigx/runtime-core';
import { Awaiting, Alerts, Bars, DetailList, Series, type DetailRow } from '../parts/primitives';
import { awaitingReason, callsTone, callsValue, panelState, type PanelProps } from './shared';

export const OverviewPanel = component<PanelProps>((ctx) => () => {
    const state = panelState(ctx.props);
    const snapshot = state.view.snapshot;
    if (!snapshot) {
        return <Awaiting alerts={alertLines(state.view)} {...awaitingReason(state)} />;
    }

    const totals = snapshot.cluster?.totals;
    const activations = totals?.activations ?? sum(snapshot.hosts, (h) => h.stats.activations);
    const queued = totals?.queued ?? sum(snapshot.hosts, (h) => h.stats.queued);
    const metrics = snapshot.metrics;
    const clusterMetrics = totals?.metrics ?? null;
    const calls = clusterMetrics?.calls ?? metrics?.calls ?? null;
    const latencyMs = clusterMetrics?.latencyMs ?? metrics?.latencyMs ?? null;
    const queueMs = clusterMetrics?.queueMs ?? metrics?.queueMs ?? null;
    const turnMs = clusterMetrics?.turnMs ?? metrics?.turnMs ?? null;
    const coverage = coverageNote(snapshot);
    // ONE ceiling across all three groups.
    const ceiling = percentileCeiling([latencyMs, queueMs, turnMs]);

    const rows: DetailRow[] = [
        { label: 'hosts', value: `${snapshot.hosts.length}` },
        { label: 'activations', value: count(activations) },
        { label: 'queued', value: count(queued) }
    ];
    if (calls) {
        rows.push({ label: 'calls', value: callsValue(calls), tone: callsTone(calls) });
    }

    return (
        <div>
            <Alerts alerts={alertLines(state.view)} />
            <p class="sxad-scope">{scopeOf(snapshot)}</p>
            <DetailList rows={rows} />
            {coverage ? <p class="sxad-note">{coverage}</p> : null}

            <section class="sxad-section">
                <h3>rates and gauges</h3>
                {/* Per SECOND. */}
                <Series
                    label="calls/s"
                    values={state.calls.values()}
                    value={rate(state.calls.latest())}
                    tone="ok"
                />
                <Series
                    label="failures/s"
                    values={state.failures.values()}
                    value={rate(state.failures.latest())}
                    tone="danger"
                />
                {/* How many there are RIGHT NOW — not a throughput. */}
                <Series
                    label="queued"
                    values={state.queued.values()}
                    value={gauge(state.queued.latest())}
                    tone="warn"
                />
                <Series
                    label="activations"
                    values={state.activations.values()}
                    value={gauge(state.activations.latest())}
                />
            </section>

            {latencyMs || queueMs || turnMs ? (
                <section class="sxad-section">
                    <h3>latency · queue · turn</h3>
                    <div class="sxad-cols">
                        <div>
                            <p class="sxad-scope">latency</p>
                            <Bars
                                points={percentilePoints(latencyMs)}
                                ceiling={ceiling}
                                format={durationMs}
                                emptyText="no samples"
                            />
                        </div>
                        <div>
                            <p class="sxad-scope">queue</p>
                            <Bars
                                points={percentilePoints(queueMs)}
                                ceiling={ceiling}
                                format={durationMs}
                                tone="warn"
                                emptyText="no samples"
                            />
                        </div>
                        <div>
                            <p class="sxad-scope">turn</p>
                            <Bars
                                points={percentilePoints(turnMs)}
                                ceiling={ceiling}
                                format={durationMs}
                                emptyText="no samples"
                            />
                        </div>
                    </div>
                    <p class="sxad-legend">
                        high queue = a hot actor · high turn = a slow method — all three share one
                        axis, because the comparison is the diagnosis
                    </p>
                </section>
            ) : (
                <p class="sxad-empty">
                    no metrics — add .use(metrics()) to see calls, latency and errors
                </p>
            )}
        </div>
    );
});

function sum<T>(items: readonly T[], of: (item: T) => number): number {
    let total = 0;
    for (const item of items) total += of(item);
    return total;
}
