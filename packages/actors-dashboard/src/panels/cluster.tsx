/** @jsxImportSource @sigx/runtime-core */
/**
 * Cluster — membership, routing counters and the reminder shard map.
 *
 * The counters are laid out in two groups and NEVER summed. `remoteDispatches`
 * beside `inboundDispatches` is the point: the gap between them is itself the
 * signal, and adding them produces a number that means nothing.
 */
import { component } from '@sigx/runtime-core';
import { alertLines, scopeOf, shardStates } from '@sigx/actors-monitor';
import { count, percent } from '@sigx/actors-monitor/format';
import { Awaiting, Alerts, DetailList, ShardGrid, type DetailRow } from '../parts/primitives';
import { awaitingReason, panelState, type PanelProps } from './shared';

export const ClusterPanel = component<PanelProps>((ctx) => () => {
    const state = panelState(ctx.props);
    const snapshot = state.view.snapshot;
    if (!snapshot) {
        return <Awaiting alerts={alertLines(state.view)} {...awaitingReason(state)} />;
    }

    const cluster = snapshot.cluster;
    if (!cluster) {
        return (
            <div>
                <Alerts alerts={alertLines(state.view)} />
                <p class="sxad-empty">single-node — no cluster to report on</p>
            </div>
        );
    }

    const c = cluster.totals.counters;
    const cacheTotal = c.routeCacheHits + c.routeCacheMisses;

    const header: DetailRow[] = [
        {
            label: 'view',
            value: `#${cluster.view.version}  ${cluster.view.active} active of ${cluster.view.size}`
        },
        { label: 'collected from', value: cluster.from },
        {
            label: 'route cache',
            value: `${percent(c.routeCacheHits, cacheTotal)} hit  (${count(cacheTotal)} lookups)`
        },
        {
            // `locateRemote / locates` is the miss rate the edge is
            // producing: high and staying high means whatever routes in front
            // of the cluster is not agreeing with placement.
            label: 'locates',
            value: `${count(c.locates)}  ${percent(c.locateRemote, c.locates)} answered "a peer owns it"`
        },
        {
            // The per-request locality fraction (#52). Read this, not
            // `routedLocal`: that one counts placement decisions and never
            // sees the warm local fast path. `?? 0` because a fleet on a
            // build that predates the pair reports neither field.
            label: 'locality',
            value: `${percent(c.dispatchesLocal ?? 0, (c.dispatchesLocal ?? 0) + (c.dispatchesRemote ?? 0))} local  (${count((c.dispatchesLocal ?? 0) + (c.dispatchesRemote ?? 0))} dispatches)`
        }
    ];

    const routing: DetailRow[] = [
        { label: 'remoteDispatches', value: count(c.remoteDispatches) },
        { label: 'inboundDispatches', value: count(c.inboundDispatches) },
        { label: 'routedLocal', value: count(c.routedLocal) },
        // A watch holds a keep-alive on the owner until the subscriber
        // leaves, so this is the number worth reading beside an activation
        // count.
        { label: 'remoteWatches', value: count(c.remoteWatches) },
        { label: 'inboundWatches', value: count(c.inboundWatches) },
        { label: 'retries', value: count(c.retries), tone: c.retries > 0 ? 'warn' : null },
        {
            label: 'routingFailures',
            value: count(c.routingFailures),
            tone: c.routingFailures > 0 ? 'danger' : null
        }
    ];

    const directory: DetailRow[] = [
        { label: 'directoryClaims', value: count(c.directoryClaims) },
        {
            label: 'claimConflicts',
            value: count(c.claimConflicts),
            tone: c.claimConflicts > 0 ? 'warn' : null
        },
        { label: 'wrongHostRedirects', value: count(c.wrongHostRedirects) },
        {
            label: 'unreachableRetries',
            value: count(c.unreachableRetries),
            tone: c.unreachableRetries > 0 ? 'warn' : null
        },
        {
            label: 'transportFallbacks',
            value: count(c.transportFallbacks),
            tone: c.transportFallbacks > 0 ? 'warn' : null
        },
        {
            label: 'authFailures',
            value: count(c.authFailures),
            tone: c.authFailures > 0 ? 'danger' : null
        },
        { label: 'selfFences', value: count(c.selfFences), tone: c.selfFences > 0 ? 'danger' : null }
    ];

    return (
        <div>
            <Alerts alerts={alertLines(state.view)} />
            <p class="sxad-scope">{scopeOf(snapshot)}</p>
            <DetailList rows={header} />
            <div class="sxad-cols sxad-section">
                <div>
                    <h3>routing</h3>
                    <DetailList rows={routing} />
                </div>
                <div>
                    <h3>directory</h3>
                    <DetailList rows={directory} />
                </div>
            </div>
            <section class="sxad-section">
                <h3>reminder shards</h3>
                <ShardGrid
                    shards={shardStates(cluster.reminderShards)}
                    emptyText="no reminder shards reported"
                />
            </section>
        </div>
    );
});
