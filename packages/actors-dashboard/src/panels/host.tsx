/** @jsxImportSource @sigx/runtime-core */
/**
 * One host, in full — the drill-down the Hosts table opens.
 *
 * Nothing on this panel is a cluster total, and it says so at the top. Every
 * figure comes from THIS host's own report: its digest, its readiness checks,
 * its recent failures, its activations.
 */
import { component } from '@sigx/runtime-core';
import { alertLines, hostTone, type HostView } from '@sigx/actors-monitor';
import { count, durationMs, uptime } from '@sigx/actors-monitor/format';
import { digestSnapshot } from '@sigx/actors/host';
import { Awaiting, Alerts, DataTable, DetailList, Section, type DetailRow } from '../parts/primitives';
import { activationColumns, activationTone } from './actors';
import { awaitingReason,
    callsTone,
    callsValue,
    checkLines,
    errorKindLines,
    recentFailureLines,
    panelState,
    type PanelProps
} from './shared';

export const HostPanel = component<PanelProps & { hostId: string }>((ctx) => () => {
    const state = panelState(ctx.props);
    const snapshot = state.view.snapshot;
    if (!snapshot) {
        return <Awaiting alerts={alertLines(state.view)} {...awaitingReason(state)} />;
    }

    const host = snapshot.hosts.find((candidate) => candidate.hostId === ctx.props.hostId);
    if (!host) {
        // It was in the last view and is not in this one. That is a fact
        // worth stating, not an empty panel.
        return (
            <div>
                <Alerts alerts={alertLines(state.view)} />
                <p class="sxad-note">{ctx.props.hostId} is no longer in the membership view</p>
                <button type="button" class="sxad-back" onClick={() => state.focus(null)}>
                    ← all hosts
                </button>
            </div>
        );
    }

    return (
        <div>
            <Alerts alerts={alertLines(state.view)} />
            <p class="sxad-bar">
                <button type="button" class="sxad-back" onClick={() => state.focus(null)}>
                    ← all hosts
                </button>
                <span class="sxad-scope">host {host.hostId} — nothing here is a cluster total</span>
            </p>
            <DetailList rows={identityRows(host)} />
            {host.metrics ? <DetailList rows={metricsRows(host)} /> : <p class="sxad-empty">no metrics from this host</p>}

            <Section title="checks" lines={checkLines(host.health)} />
            <Section title="errors by kind" lines={errorKindLines(host.metrics)} />
            <Section
                title="recent failures"
                lines={recentFailureLines(host.metrics?.errors.recent ?? [])}
            />

            <section class="sxad-section">
                <h3>actors on this host</h3>
                {host.activations ? (
                    <DataTable
                        columns={activationColumns}
                        rows={host.activations}
                        tone={activationTone}
                        emptyText="no live activations"
                    />
                ) : (
                    // The detail poll is issued the moment a drill-down
                    // opens, but the panel renders before it lands. Saying
                    // "no actors" here would read as a broken host.
                    <p class="sxad-empty">waiting for a detail poll…</p>
                )}
            </section>
        </div>
    );
});

function identityRows(host: HostView): DetailRow[] {
    const rows: DetailRow[] = [
        { label: 'status', value: host.status, tone: hostTone(host.status) },
        { label: 'address', value: host.address },
        { label: 'up', value: uptime(host.uptimeMs) }
    ];
    if (host.health) {
        rows.push({
            label: 'ready',
            value: host.health.fatal ? 'FATAL' : host.health.ready ? 'yes' : 'NO',
            tone: host.health.fatal ? 'danger' : host.health.ready ? 'ok' : 'warn'
        });
    }
    return rows;
}

function metricsRows(host: HostView): DetailRow[] {
    const digest = host.metrics!;
    // The raw log-linear buckets, read back as percentiles.
    const latency = digest.latency ? digestSnapshot(digest.latency) : null;
    return [
        { label: 'calls', value: callsValue(digest.calls), tone: callsTone(digest.calls) },
        {
            label: 'latency',
            value: latency
                ? `p50 ${durationMs(latency.p50Ms)}  p99 ${durationMs(latency.p99Ms)}`
                : 'no samples'
        },
        {
            label: 'storage',
            value:
                `${count(digest.storage.loads)} loads  ${count(digest.storage.saves)} saves  ` +
                `${count(digest.storage.conflicts)} conflicts`,
            // Every etag conflict discarded an activation's work.
            tone: digest.storage.conflicts > 0 ? 'warn' : null
        }
    ];
}
