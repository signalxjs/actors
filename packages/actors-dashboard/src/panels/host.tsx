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
import { bytes, count, durationMs, uptime } from '@sigx/actors-monitor/format';
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
            {/* Socket sessions (#166) — only when this host reported them.
                "Said nothing" is not "no sockets": a host without a socket
                mount and one whose sessions all closed are different
                findings, and today only the polled host can say anything. */}
            {host.sockets ? (
                <section class="sxad-section">
                    <h3>socket sessions</h3>
                    <DetailList rows={socketRows(host)} />
                </section>
            ) : null}

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
        // Where it runs, when the chart said (#51) — beside the address,
        // which is the other half of "where is this host".
        ...(host.meta?.node ? [{ label: 'node', value: host.meta.node }] : []),
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

function socketRows(host: HostView): DetailRow[] {
    const sockets = host.sockets!;
    return [
        {
            label: 'sockets',
            value:
                `${count(sockets.open)} open  ${count(sockets.inFlight)} in flight  ${count(sockets.subscriptions)} subs` +
                (sockets.throttleQuantized > 0
                    ? `  ${count(sockets.throttleQuantized)} throttle-quantized`
                    : '')
        },
        {
            label: 'deliveries',
            value: `${count(sockets.deliveries)} frames  ${bytes(sockets.deliveryBytes)}`
        },
        // `—` is "no adapter could tell us", which is not `0 B` (#208).
        { label: 'buffered', value: bytes(sockets.bufferedBytes) },
        {
            label: 'connections',
            value: `${count(sockets.connectionsOpened)} opened  ${count(sockets.connectionsClosed)} closed  ${count(sockets.connectionsRefused)} refused`,
            // A refused upgrade is a client that could not get in.
            tone: sockets.connectionsRefused > 0 ? 'warn' : null
        },
        {
            // Closes the HOST decided on — a lifetime cap or a protocol
            // breach — as opposed to a client hanging up.
            label: 'evicted',
            value: `${count(sockets.lifetimeCloses)} lifetime  ${count(sockets.protocolBreaches)} protocol breach`,
            tone: sockets.protocolBreaches > 0 ? 'warn' : null
        },
        {
            label: 'lifetime',
            value: sockets.lifetimeMs
                ? `p50 ${durationMs(sockets.lifetimeMs.p50Ms)}  p99 ${durationMs(sockets.lifetimeMs.p99Ms)}`
                : 'no samples'
        }
    ];
}
