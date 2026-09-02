/** @jsxImportSource @sigx/runtime-core */
/**
 * Hosts — the fleet, one row each, and the drill-down behind it.
 *
 * The READY column is per-host rather than "the polled host's readiness
 * repeated": a fleet with one fenced peer used to read as healthy because
 * only one host's health was visible anywhere. `FATAL` is not "very not
 * ready" — it says this host identity is unrecoverable and the pod must be
 * REPLACED, and reading it as draining is how a zombie sits there forever.
 *
 * Clicking a row opens `HostPanel`. That is not just a different rendering:
 * it changes what is REQUESTED. A detail poll makes the selected host walk
 * its activation table, so nobody pays for it while the panel is closed —
 * which is why the click goes through `state.focus()` rather than through
 * local view state.
 */
import { component } from '@sigx/runtime-core';
import { alertLines, hostTone, nodeLabels, type HostView } from '@sigx/actors-monitor';
import { count, uptime } from '@sigx/actors-monitor/format';
import { Awaiting, Alerts, DataTable, Section, type Column, type Tone } from '../parts/primitives';
import { awaitingReason, panelState, readyWord, type PanelProps } from './shared';

/**
 * The table's columns. A function of the fleet, not a constant, because the
 * node cell is a label derived across every host (below), and because the
 * sockets column exists only when some host reported one.
 */
const columns = (
    labels: ReadonlyMap<string, string>,
    sockets: boolean
): readonly Column<HostView>[] => [
    { key: 'id', header: 'host', value: (h) => h.hostId, key_: true },
    { key: 'status', header: 'status', value: (h) => h.status },
    { key: 'ready', header: 'ready', value: readyWord },
    { key: 'up', header: 'up', value: (h) => uptime(h.uptimeMs), numeric: true },
    { key: 'acts', header: 'acts', value: (h) => count(h.stats.activations), numeric: true },
    { key: 'queue', header: 'queue', value: (h) => count(h.stats.queued), numeric: true },
    // Open socket sessions (#166), only when some host reported them — a
    // column of `—` would say "no sockets anywhere" about a fleet that never
    // said. `—` on a peer means the fan-out carried nothing for it, which
    // today it never does: only the polled host can report.
    ...(sockets
        ? [
              {
                  key: 'sockets',
                  header: 'sockets',
                  value: (h: HostView) => (h.sockets ? count(h.sockets.open) : '—'),
                  numeric: true
              }
          ]
        : []),
    {
        key: 'view',
        header: 'view',
        // A spread across hosts means the membership view has not converged,
        // which usually explains every other disagreement on screen.
        value: (h) => (h.membershipVersion === null ? '—' : `#${h.membershipVersion}`),
        numeric: true
    },
    {
        key: 'tx',
        header: 'transports',
        // A disagreement here is a half-rolled transport deploy, which is
        // usually the whole story.
        value: (h) => h.transports?.join(',') ?? '—'
    },
    {
        key: 'node',
        header: 'node',
        // The machine under the pod, from `PlacementOptions.meta.node` — the
        // column that turns `3/3 replicas` into "all three on one node"
        // (#51). The cell is the monitor's LABEL (`…vmss000001`, the tail
        // that differs), not the raw name: a `key_` cell ellipsises from
        // the right at 28ch, real node names differ only in their tail,
        // and two different nodes cut to `aks-sigxactors-12345678-vms…`
        // would read as one — the inverse of the finding. The same label
        // repeated down the column is still the finding; the full name is
        // the tooltip and the drill-down.
        value: (h) => (h.meta?.node ? (labels.get(h.meta.node) ?? h.meta.node) : '—'),
        title: (h) => h.meta?.node,
        key_: true
    }
];

/**
 * Row tone. A host whose readiness has FAILED is worth colouring even when
 * its membership status is a healthy `active` — those are different
 * questions, and the second one going wrong is invisible in the first.
 */
function rowTone(host: HostView): Tone | null {
    const status = hostTone(host.status);
    if (status) return status;
    if (host.health?.fatal) return 'danger';
    if (host.health && !host.health.ready) return 'warn';
    return null;
}

export const HostsPanel = component<PanelProps>((ctx) => () => {
    const state = panelState(ctx.props);
    const snapshot = state.view.snapshot;
    if (!snapshot) {
        return <Awaiting alerts={alertLines(state.view)} {...awaitingReason(state)} />;
    }

    const unreachable = (snapshot.cluster?.unreachable ?? []).map(
        (failure) => `${failure.hostId}  ${failure.address}  ${failure.reason} — ${failure.message}`
    );

    return (
        <div>
            <Alerts alerts={alertLines(state.view)} />
            <DataTable
                columns={columns(
                    nodeLabels(snapshot.hosts),
                    snapshot.hosts.some((host) => host.sockets !== null)
                )}
                rows={snapshot.hosts}
                tone={rowTone}
                onPick={(host) => state.focus(host.hostId)}
                pickLabel={(host) => `open host ${host.hostId}`}
                emptyText="no hosts"
            />
            {/* A host that did not answer is why every total above is a lower
                bound. It gets its own block rather than a missing row. */}
            <Section title="unreachable" lines={unreachable} tone="danger" />
        </div>
    );
});
