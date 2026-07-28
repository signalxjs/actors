/** @jsxImportSource @sigx/terminal */
/**
 * The dashboard's tabs.
 *
 * Each is a pure function of `DashboardState`. What each screen chooses to
 * put FIRST is the design work here — a dashboard's job is not to show
 * everything, it is to make the thing that is wrong impossible to miss.
 */
import type { ActivationInfo } from '@sigx/actors/silo';
import { count, durationMs, percent, uptime } from '../model/format';
import { commonScale, shardGrid, splitShards, unclaimedShards } from '../tui/bars';
import {
    DataTable,
    HistogramBars,
    KeyValue,
    ShardGrid,
    Sparkline
} from '../tui/components';
import type { Column } from '../tui/table';
import type { SiloView } from '../source/types';
import type { DashboardState } from './state';

/** Status tones. `fenced` and `leaving` must never look like `active`. */
function siloTone(status: string): string | undefined {
    if (status === 'active' || status === 'unknown') return undefined;
    if (status === 'fenced') return 'danger';
    if (status === 'leaving') return 'warn';
    return 'dim';
}

/** A banner for whatever is currently wrong, or nothing. */
function Alerts(props: { state: DashboardState }) {
    const { snapshot, error, paused } = props.state.view;
    const lines: { text: string; tone: string }[] = [];

    if (error) {
        // The numbers below are still the last good ones. Say so, rather
        // than letting them read as current.
        lines.push({ text: `poll failed — showing the last good snapshot: ${error}`, tone: 'danger' });
    }
    if (paused) lines.push({ text: 'PAUSED — press p to resume', tone: 'warn' });
    if (snapshot?.partial) {
        lines.push({
            text: 'PARTIAL — a silo did not answer, so every total is a LOWER BOUND',
            tone: 'warn'
        });
    }
    const shards = snapshot?.cluster?.reminderShards;
    if (shards) {
        const empty = unclaimedShards(shards);
        const split = splitShards(shards);
        if (empty.length > 0) {
            lines.push({
                text: `${empty.length} reminder shard(s) UNCLAIMED (${empty.join(' ')}) — nothing is ticking them`,
                tone: 'danger'
            });
        }
        if (split.length > 0) {
            lines.push({
                text: `${split.length} reminder shard(s) claimed twice — membership views have diverged`,
                tone: 'warn'
            });
        }
    }
    const fenced = snapshot?.silos.filter((s) => s.status === 'fenced') ?? [];
    if (fenced.length > 0) {
        // The one a load balancer cannot see: a fenced silo still publishes
        // `active` while refusing every activation.
        lines.push({
            text: `${fenced.length} silo(s) FENCED — refusing activations while still published as active`,
            tone: 'danger'
        });
    }
    const auth = totalCounter(snapshot?.silos ?? [], 'authFailures');
    if (auth > 0) {
        lines.push({
            text: `${count(auth)} cluster auth failure(s) — a secret rotation has not reached every silo`,
            tone: 'danger'
        });
    }

    if (lines.length === 0) return null;
    return (
        <box border="none">
            {lines.map((line) => (
                <text color={line.tone} bold>
                    ! {line.text}
                </text>
            ))}
            <br />
        </box>
    );
}

function totalCounter(silos: readonly SiloView[], key: 'authFailures'): number {
    let total = 0;
    for (const silo of silos) total += silo.counters?.[key] ?? 0;
    return total;
}

export function OverviewScreen(props: { state: DashboardState }) {
    const state = props.state;
    const snapshot = state.view.snapshot;
    if (!snapshot) return <text color="dim">connecting…</text>;

    const totals = snapshot.cluster?.totals;
    const activations = totals?.activations ?? sum(snapshot.silos, (s) => s.stats.activations);
    const queued = totals?.queued ?? sum(snapshot.silos, (s) => s.stats.queued);
    const metrics = snapshot.metrics;
    const scale = commonScale([metrics?.latencyMs ?? null, metrics?.queueMs ?? null, metrics?.turnMs ?? null]);

    return (
        <box border="none">
            <Alerts state={state} />
            <KeyValue
                rows={[
                    { label: 'silos', value: `${snapshot.silos.length}` },
                    { label: 'activations', value: count(activations) },
                    { label: 'queued', value: count(queued) },
                    ...(metrics
                        ? [
                              {
                                  label: 'calls',
                                  value: `${count(metrics.calls.total)}  ${count(metrics.calls.failed)} failed (${percent(metrics.calls.failed, metrics.calls.total)})`,
                                  tone: metrics.calls.failed > 0 ? 'warn' : undefined
                              }
                          ]
                        : [])
                ]}
            />
            <br />
            <Sparkline label="calls/s" values={state.calls.values()} value={rateText(state.calls.latest())} />
            <Sparkline
                label="failures/s"
                values={state.failures.values()}
                value={rateText(state.failures.latest())}
                color="danger"
            />
            <Sparkline label="queued" values={state.queued.values()} value={count(queued)} color="warn" />
            <Sparkline label="activations" values={state.activations.values()} value={count(activations)} />
            {metrics ? (
                <box border="none">
                    <br />
                    {/* One shared scale: the comparison IS the diagnosis. */}
                    <HistogramBars label="latency" snapshot={metrics.latencyMs} scaleMs={scale} format={durationMs} />
                    <HistogramBars label="queue" snapshot={metrics.queueMs} scaleMs={scale} format={durationMs} color="warn" />
                    <HistogramBars label="turn" snapshot={metrics.turnMs} scaleMs={scale} format={durationMs} color="accent" />
                    <text color="dim">high queue = a hot grain · high turn = a slow method</text>
                </box>
            ) : (
                <text color="dim">no metrics — add .use(metrics()) to see calls, latency and errors</text>
            )}
        </box>
    );
}

const siloColumns: Column<SiloView>[] = [
    { key: 'id', header: 'SILO', value: (s) => s.siloId, min: 8 },
    { key: 'status', header: 'STATUS', value: (s) => s.status },
    { key: 'up', header: 'UP', value: (s) => uptime(s.uptimeMs), align: 'right' },
    { key: 'act', header: 'ACTS', value: (s) => count(s.stats.activations), align: 'right' },
    { key: 'queued', header: 'QUEUE', value: (s) => count(s.stats.queued), align: 'right' },
    {
        key: 'view',
        header: 'VIEW',
        value: (s) => (s.membershipVersion === null ? '—' : `#${s.membershipVersion}`),
        align: 'right'
    },
    { key: 'tx', header: 'TRANSPORTS', value: (s) => s.transports?.join(',') ?? '—' }
];

export function SilosScreen(props: { state: DashboardState; cursor: number; width?: number }) {
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <text color="dim">connecting…</text>;
    const unreachable = snapshot.cluster?.unreachable ?? [];
    return (
        <box border="none">
            <Alerts state={props.state} />
            <DataTable
                columns={siloColumns}
                rows={snapshot.silos}
                cursor={props.cursor}
                width={props.width}
                tone={(silo) => siloTone(silo.status)}
            />
            {unreachable.length > 0 ? (
                <box border="none">
                    <br />
                    <text color="danger" bold>
                        unreachable
                    </text>
                    {unreachable.map((failure) => (
                        <text color="danger">
                            {`  ${failure.siloId}  ${failure.address}  ${failure.reason} — ${failure.message}`}
                        </text>
                    ))}
                </box>
            ) : null}
        </box>
    );
}

const grainColumns: Column<ActivationInfo>[] = [
    { key: 'type', header: 'TYPE', value: (g) => g.type },
    { key: 'key', header: 'KEY', value: (g) => g.key, min: 10 },
    { key: 'queued', header: 'QUEUE', value: (g) => String(g.queued), align: 'right' },
    { key: 'age', header: 'AGE', value: (g) => uptime(g.ageMs), align: 'right' },
    { key: 'idle', header: 'IDLE', value: (g) => uptime(g.idleMs), align: 'right' },
    { key: 'alive', header: 'KEPT', value: (g) => (g.keptAlive ? 'yes' : '') }
];

export function GrainsScreen(props: { state: DashboardState; cursor: number; width?: number }) {
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <text color="dim">connecting…</text>;
    const metrics = snapshot.metrics;
    const byMethod = metrics
        ? Object.entries(metrics.byMethod)
              .sort((a, b) => (b[1].turnMs?.p99Ms ?? 0) - (a[1].turnMs?.p99Ms ?? 0))
              .slice(0, 8)
        : [];

    return (
        <box border="none">
            <Alerts state={props.state} />
            {snapshot.activations ? (
                <DataTable
                    columns={grainColumns}
                    rows={snapshot.activations}
                    cursor={props.cursor}
                    width={props.width}
                    tone={(grain) => (grain.queued > 0 ? 'warn' : undefined)}
                />
            ) : (
                <text color="dim">no activation list — the source reports none</text>
            )}
            {byMethod.length > 0 ? (
                <box border="none">
                    <br />
                    <text color="dim" bold>
                        slowest methods (p99 turn)
                    </text>
                    {byMethod.map(([name, m]) => (
                        <text>
                            {`  ${name.padEnd(30)} ${durationMs(m.turnMs?.p99Ms ?? 0).padStart(9)}  ${count(m.calls)} calls  ${m.failed > 0 ? `${count(m.failed)} failed` : ''}`}
                        </text>
                    ))}
                </box>
            ) : null}
        </box>
    );
}

export function ClusterScreen(props: { state: DashboardState }) {
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <text color="dim">connecting…</text>;
    const cluster = snapshot.cluster;
    if (!cluster) {
        return (
            <box border="none">
                <Alerts state={props.state} />
                <text color="dim">single-node — no cluster to report on</text>
            </box>
        );
    }
    const c = cluster.totals.counters;
    const cacheTotal = c.routeCacheHits + c.routeCacheMisses;

    return (
        <box border="none">
            <Alerts state={props.state} />
            <KeyValue
                labelWidth={20}
                rows={[
                    { label: 'view', value: `#${cluster.view.version}  ${cluster.view.active} active of ${cluster.view.size}` },
                    { label: 'collected from', value: cluster.from },
                    // Side by side and NEVER summed: the gap between them is
                    // itself the signal.
                    { label: 'remoteDispatches', value: count(c.remoteDispatches) },
                    { label: 'inboundDispatches', value: count(c.inboundDispatches) },
                    { label: 'routedLocal', value: count(c.routedLocal) },
                    { label: 'retries', value: count(c.retries), tone: c.retries > 0 ? 'warn' : undefined },
                    { label: 'routingFailures', value: count(c.routingFailures), tone: c.routingFailures > 0 ? 'danger' : undefined },
                    { label: 'route cache', value: `${percent(c.routeCacheHits, cacheTotal)} hit  (${count(cacheTotal)} lookups, ${count(c.routeCacheSize)} entries)` },
                    { label: 'directoryClaims', value: count(c.directoryClaims) },
                    { label: 'claimConflicts', value: count(c.claimConflicts), tone: c.claimConflicts > 0 ? 'warn' : undefined },
                    { label: 'wrongHostRedirects', value: count(c.wrongHostRedirects) },
                    { label: 'unreachableRetries', value: count(c.unreachableRetries), tone: c.unreachableRetries > 0 ? 'warn' : undefined },
                    { label: 'transportFallbacks', value: count(c.transportFallbacks), tone: c.transportFallbacks > 0 ? 'warn' : undefined },
                    { label: 'authFailures', value: count(c.authFailures), tone: c.authFailures > 0 ? 'danger' : undefined },
                    { label: 'selfFences', value: count(c.selfFences), tone: c.selfFences > 0 ? 'danger' : undefined }
                ]}
            />
            <br />
            <ShardGrid rows={shardGrid(cluster.reminderShards, 8)} />
        </box>
    );
}

export function HealthScreen(props: { state: DashboardState }) {
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <text color="dim">connecting…</text>;
    const health = snapshot.health;
    const metrics = snapshot.metrics;

    return (
        <box border="none">
            <Alerts state={props.state} />
            {health ? (
                <box border="none">
                    <KeyValue
                        rows={[
                            { label: 'live', value: health.live ? 'yes' : 'no', tone: health.live ? 'success' : 'danger' },
                            { label: 'ready', value: health.ready ? 'yes' : 'NO', tone: health.ready ? 'success' : 'warn' },
                            { label: 'uptime', value: uptime(health.uptimeMs) }
                        ]}
                    />
                    {health.live && !health.ready ? (
                        // The distinction the whole endpoint exists for.
                        <text color="warn" bold>
                            ALIVE but out of rotation — drain it, do not restart it
                        </text>
                    ) : null}
                    <br />
                    {Object.entries(health.checks).map(([name, check]) => (
                        <text color={check.ready ? 'success' : 'danger'}>
                            {`  ${check.ready ? 'ok  ' : 'FAIL'} ${name}${check.detail ? ` — ${check.detail}` : ''}`}
                        </text>
                    ))}
                    {Object.keys(health.checks).length === 0 ? (
                        <text color="dim">  no readiness checks contributed</text>
                    ) : null}
                </box>
            ) : (
                <text color="dim">no health status — export an ops() or health() handle</text>
            )}
            {metrics ? (
                <box border="none">
                    <br />
                    <text color="dim" bold>
                        errors by kind
                    </text>
                    {Object.entries(metrics.errors.byKind).length === 0 ? (
                        <text color="dim">  none</text>
                    ) : (
                        Object.entries(metrics.errors.byKind)
                            .sort((a, b) => (b[1] ?? 0) - (a[1] ?? 0))
                            .map(([kind, n]) => (
                                <text color={kind === '(unknown)' ? 'fg' : 'warn'}>
                                    {`  ${kind.padEnd(20)} ${count(n ?? 0)}`}
                                </text>
                            ))
                    )}
                    {metrics.errors.recent.length > 0 ? (
                        <box border="none">
                            <br />
                            <text color="dim" bold>
                                recent failures
                            </text>
                            {metrics.errors.recent
                                .slice(-6)
                                .reverse()
                                .map((entry) => (
                                    <text color="dim">
                                        {`  ${new Date(entry.at).toISOString().slice(11, 19)} ${entry.type}#${entry.method} ${entry.kind}: ${entry.message}`}
                                    </text>
                                ))}
                        </box>
                    ) : null}
                    <br />
                    <text color="dim" bold>
                        storage
                    </text>
                    <text>
                        {`  ${count(metrics.storage.loads)} loads  ${count(metrics.storage.saves)} saves  ${count(metrics.storage.clears)} clears`}
                    </text>
                    <text color={metrics.storage.conflicts > 0 ? 'warn' : 'fg'}>
                        {`  ${count(metrics.storage.conflicts)} etag conflicts — each one discarded an activation`}
                    </text>
                </box>
            ) : null}
        </box>
    );
}

function sum<T>(items: readonly T[], of: (item: T) => number): number {
    let total = 0;
    for (const item of items) total += of(item);
    return total;
}

function rateText(value: number | null): string {
    return value === null ? '—' : `${count(value)}/s`;
}
