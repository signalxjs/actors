/** @jsxImportSource @sigx/terminal */
/**
 * The dashboard's tabs.
 *
 * Each is a pure function of `DashboardState` and the PANE it was given.
 * What each screen chooses to put FIRST is the design work here — a
 * dashboard's job is not to show everything, it is to make the thing that
 * is wrong impossible to miss.
 *
 * Every screen is width- AND height-aware. `@sigx/cli` 0.9 hands a tab's
 * `render` the content box left after the shell's own chrome
 * (signalxjs/cli#88), which is the number a table needs to size its
 * viewport and a banner needs to wrap against. Before that, nothing here
 * knew the width at all: content clipped at the right edge while most of
 * the screen sat empty.
 *
 * Three rules follow from having a budget rather than a canvas:
 *
 *   - **Cells truncate, prose wraps.** Cutting an alert at the pane edge
 *     throws away the half that says what to do about it.
 *   - **Every block states its height in lines**, so the table can be given
 *     exactly the rows that are left. A screen that overflows is not
 *     clipped harmlessly — the shell keeps the TOP, so it is the last rows
 *     that silently vanish, and those are the ones a scrolling table needs.
 *   - **Sections are flat, not nested `Col`s.** A `Col` inside a `Col`
 *     renders one extra blank line, which is invisible until it is the line
 *     that pushes the bottom of the table off the pane. Blocks are built as
 *     arrays of lines so their height is exactly what it says.
 */
import {
    BarChart,
    Col,
    DataTable,
    DetailList,
    Heading,
    Row,
    Sparkline,
    StatusGrid,
    Trend,
    fitCell,
    type Model,
    type TableColumn
} from '@sigx/terminal';
import { digestSnapshot, type ActivationInfo } from '@sigx/actors/host';
import { count, durationMs, gauge, percent, rate, uptime } from '@sigx/actors-monitor/format';
import {
    alertLines,
    coverageNote,
    hostTone,
    nodeCount,
    nodeLabels,
    polledLabel,
    scopeOf,
    type Alert,
    type DashboardState,
    type HostView
} from '@sigx/actors-monitor';
import { ALERT_COLOR, histogramScale, percentileItems, shardCells } from '../tui/bars';
import { wrapText } from '../tui/wrap';
import { Line } from '../tui/components';

/**
 * The room a screen has to fill.
 *
 * Structurally `ShellPane` from `@sigx/cli`, restated so the dashboard can
 * be rendered and tested without the shell — the same reason
 * `src/source/http.ts` restates the ops payload.
 */
export interface Pane {
    width: number;
    height: number;
}

/** What a screen assumes when nobody says — a conservative 80×24. */
export const DEFAULT_PANE: Pane = { width: 80, height: 24 };

/** Never let a table collapse to nothing, however cramped the pane. */
const MIN_TABLE_ROWS = 3;
/**
 * What `DataTable` draws around its rows: a header, a rule, a footer and
 * the blank that follows it. Counted here because it PADS its body out to
 * the height it is given, so the arithmetic has to be right in both
 * directions — too small wastes the pane, too large loses the footer.
 */
const TABLE_CHROME = 4;
/** `DataTable` draws a cursor gutter to the left of the width it is given. */
const TABLE_GUTTER = 1;

const LABEL_WIDTH = 12;

/**
 * Each alert WRAPPED to the pane, continuation lines indented under it.
 *
 * The list itself is `@sigx/actors-monitor`'s — what is wrong, and how badly,
 * is not a terminal question. Wrapping it is: prose cut at the pane edge
 * throws away the half that says what to do about it, and a browser has no
 * equivalent problem.
 */
function wrappedAlerts(state: DashboardState, pane: Pane): Alert[] {
    return alertLines(state.view).flatMap((alert) =>
        wrapText(`! ${alert.text}`, pane.width, '  ').map((text) => ({ text, tone: alert.tone }))
    );
}

/** A banner for whatever is currently wrong, plus a blank — or nothing. */
function alerts(state: DashboardState, pane: Pane): unknown[] {
    const lines = wrappedAlerts(state, pane);
    if (lines.length === 0) return [];
    return [
        ...lines.map((line) => (
            <Line color={ALERT_COLOR[line.tone]} bold>
                {line.text}
            </Line>
        )),
        <br />
    ];
}

/** Rows an alert banner will occupy, so a screen can budget for it. */
function alertHeight(state: DashboardState, pane: Pane): number {
    const lines = wrappedAlerts(state, pane).length;
    return lines === 0 ? 0 : lines + 1;
}

/** `hostTone` speaks severities; `DataTable` speaks theme colours. */
function hostColor(status: string): string | undefined {
    return hostTone(status) ?? undefined;
}

/**
 * A titled block: a blank, a heading, then pre-formatted lines fitted to
 * the pane.
 *
 * Built from an array rather than from JSX children, and returned FLAT
 * rather than wrapped in a `Col`, so its height is `lines.length + 2`
 * exactly — which is what makes every table's budget honest.
 */
function block(title: string, lines: readonly string[], pane: Pane, color = 'dim'): unknown[] {
    if (lines.length === 0) return [];
    return [
        <br />,
        <Heading color={color}>{title}</Heading>,
        ...lines.map((line) => <Line color={color === 'dim' ? undefined : color}>{fitCell(line, pane.width)}</Line>)
    ];
}

const blockHeight = (lines: readonly string[]): number => (lines.length === 0 ? 0 : lines.length + 2);

// `scopeOf`, `polledLabel` and `coverageNote` are `@sigx/actors-monitor`'s
// (#239). What a number is ABOUT — this host or the fleet, and whether the
// fleet's total covers all of it — is the work #121 exists over, and it is
// not a terminal question: the failure it fixes was never a wrong number, it
// was a right one under no label at all, sitting directly beneath one of a
// different scope. A second renderer must inherit those strings, not re-word
// them.

export function OverviewScreen(props: { state: DashboardState; pane?: Pane }) {
    const state = props.state;
    const pane = props.pane ?? DEFAULT_PANE;
    const snapshot = state.view.snapshot;
    if (!snapshot) return <Line color="dim">connecting…</Line>;

    const totals = snapshot.cluster?.totals;
    const activations = totals?.activations ?? sum(snapshot.hosts, (s) => s.stats.activations);
    const queued = totals?.queued ?? sum(snapshot.hosts, (s) => s.stats.queued);
    const metrics = snapshot.metrics;
    // Cluster-wide numbers when the fan-out produced them, this host's
    // otherwise — and the heading says which, because printing one under a
    // label that means the other is the whole complaint behind #121.
    const clusterMetrics = snapshot.cluster?.totals.metrics ?? null;
    const clusterCalls = clusterMetrics?.calls ?? null;
    const latencyMs = clusterMetrics?.latencyMs ?? metrics?.latencyMs ?? null;
    const queueMs = clusterMetrics?.queueMs ?? metrics?.queueMs ?? null;
    const turnMs = clusterMetrics?.turnMs ?? metrics?.turnMs ?? null;
    const coverage = coverageNote(snapshot);
    const scale = histogramScale([latencyMs, queueMs, turnMs]);
    // Distinct machines under those hosts, when the chart says. Three hosts
    // on one node is the finding this row exists for (#51); no row at all
    // when nothing reported a node, rather than a guess either way.
    const nodes = nodeCount(snapshot.hosts);

    // The series occupy the width the pane has, less the label column and
    // the value that trails each one — but never more than the history
    // they can hold. `pad` right-aligns, so a 24-sample series drawn into
    // 72 cells is 48 blanks between the label and the line, with the value
    // stranded off to the right. Capping at the capacity keeps the newest
    // sample at a STABLE right edge (the reason to pad at all) without
    // opening a gap the data can never fill.
    const sparkWidth = Math.max(8, Math.min(pane.width - LABEL_WIDTH - 16, state.calls.capacity));
    // Three histogram groups side by side, each a label, a bar and a figure.
    const barWidth = Math.max(6, Math.floor((pane.width - 4) / 3) - 14);

    return (
        <Col>
            {alerts(state, pane)}
            <Heading color="dim">{scopeOf(snapshot)}</Heading>
            <DetailList
                labelWidth={LABEL_WIDTH}
                rows={[
                    { label: 'hosts', value: `${snapshot.hosts.length}` },
                    ...(nodes !== null ? [{ label: 'nodes', value: `${nodes}` }] : []),
                    { label: 'activations', value: count(activations) },
                    { label: 'queued', value: count(queued) },
                    ...(clusterCalls
                        ? [
                              {
                                  label: 'calls',
                                  value:
                                      `${count(clusterCalls.total)}  ${count(clusterCalls.failed)} failed (${percent(clusterCalls.failed, clusterCalls.total)})` +
                                      (clusterCalls.oneWayFailures > 0
                                          ? `  ${count(clusterCalls.oneWayFailures)} one-way`
                                          : ''),
                                  tone:
                                      clusterCalls.failed > 0 || clusterCalls.oneWayFailures > 0
                                          ? 'warn'
                                          : undefined
                              }
                          ]
                        : metrics
                          ? [
                                {
                                    label: 'calls',
                                    value:
                                        `${count(metrics.calls.total)}  ${count(metrics.calls.failed)} failed (${percent(metrics.calls.failed, metrics.calls.total)})` +
                                        ((metrics.calls.oneWayFailures ?? 0) > 0
                                            ? `  ${count(metrics.calls.oneWayFailures ?? 0)} one-way`
                                            : ''),
                                    tone:
                                        metrics.calls.failed > 0 ||
                                        (metrics.calls.oneWayFailures ?? 0) > 0
                                            ? 'warn'
                                            : undefined
                                }
                            ]
                          : [])
                ]}
            />
            {coverage ? <Line color="warn">{fitCell(coverage, pane.width)}</Line> : null}
            <br />
            {/* Rising is not the same news for every metric, so each series
                states its own polarity: more calls is good, more failures,
                queue depth or activations is not. A `▲` coloured as a
                warning on a throughput line lies half the time.

                And a rate is not a gauge: calls and failures are per
                second; queue depth and activation count are simply how many
                there are right now. Labelling either as the other is the
                same class of lie. */}
            {series('calls/s', state.calls, sparkWidth, rate, 'accent', 'higher-is-better')}
            {series('failures/s', state.failures, sparkWidth, rate, 'danger')}
            {series('queued', state.queued, sparkWidth, gauge, 'warn')}
            {series('activations', state.activations, sparkWidth, gauge, 'accent')}
            {latencyMs || queueMs || turnMs
                ? [
                      <br />,
                      /* ONE shared scale across all three: the comparison is
                         the diagnosis, and a per-group scale would draw a
                         12µs queue wait and a 47ms turn identically. */
                      <Row gap={2}>
                          <Col>
                              <Heading color="dim">latency</Heading>
                              {percentiles(latencyMs, scale, barWidth, 'accent')}
                          </Col>
                          <Col>
                              <Heading color="dim">queue</Heading>
                              {percentiles(queueMs, scale, barWidth, 'warn')}
                          </Col>
                          <Col>
                              <Heading color="dim">turn</Heading>
                              {percentiles(turnMs, scale, barWidth, 'accent')}
                          </Col>
                      </Row>,
                      <Line color="dim">
                          {fitCell('high queue = a hot actor · high turn = a slow method', pane.width)}
                      </Line>
                  ]
                : [<Line color="dim">no metrics — add .use(metrics()) to see calls, latency and errors</Line>]}
        </Col>
    );
}

/** One time series: the sparkline, then its current value and direction. */
function series(
    label: string,
    source: DashboardState['calls'],
    width: number,
    format: (value: number | null) => string,
    color: string,
    polarity: 'higher-is-better' | 'higher-is-worse' | 'neutral' = 'higher-is-worse'
): unknown {
    const values = source.values();
    const current = source.latest();
    const previous = values.length > 1 ? (values[values.length - 2] ?? null) : null;
    return (
        <Row gap={1}>
            <Col>
                <Sparkline
                    label={label}
                    labelWidth={LABEL_WIDTH}
                    width={width}
                    values={values}
                    color={color}
                    pad
                />
            </Col>
            <Col>
                <Trend current={current} previous={previous} value={format(current)} polarity={polarity} />
            </Col>
        </Row>
    );
}

/** p50 / p90 / p99 as three bars against a scale the CALLER owns. */
function percentiles(
    snapshot: Parameters<typeof percentileItems>[0],
    scale: number,
    width: number,
    color: string
): unknown {
    return (
        <BarChart
            items={percentileItems(snapshot)}
            scale={scale}
            width={width}
            labelWidth={3}
            format={durationMs}
            color={color}
            emptyText="no samples"
        />
    );
}

/**
 * The host table's columns. A function of the fleet, not a constant,
 * because the NODE cell is a label derived across every host (below).
 */
const hostColumns = (labels: ReadonlyMap<string, string>): TableColumn<HostView>[] => [
    { key: 'id', header: 'HOST', value: (s) => s.hostId, min: 8 },
    { key: 'status', header: 'STATUS', value: (s) => s.status },
    {
        // Every host's readiness, not just the polled one's — the column
        // that was impossible before `HostReport` carried health. `FATAL`
        // is not "very not ready": it means this host cannot recover and
        // must be REPLACED, and reading it as draining is how a zombie pod
        // sits there forever.
        key: 'ready',
        header: 'READY',
        value: (s) => (!s.health ? '—' : s.health.fatal ? 'FATAL' : s.health.ready ? 'yes' : 'NO'),
        color: (s) =>
            !s.health ? 'dim' : s.health.fatal ? 'danger' : s.health.ready ? undefined : 'warn'
    },
    { key: 'up', header: 'UP', value: (s) => uptime(s.uptimeMs), align: 'right' },
    { key: 'act', header: 'ACTS', value: (s) => count(s.stats.activations), align: 'right' },
    { key: 'queued', header: 'QUEUE', value: (s) => count(s.stats.queued), align: 'right' },
    {
        key: 'view',
        header: 'VIEW',
        value: (s) => (s.membershipVersion === null ? '—' : `#${s.membershipVersion}`),
        align: 'right'
    },
    { key: 'tx', header: 'TRANSPORTS', value: (s) => s.transports?.join(',') ?? '—' },
    {
        // The machine under the pod, from `PlacementOptions.meta.node` —
        // the column that turns `3/3 replicas` into "all three on one
        // node" (#51). Last, because node names are long and `DataTable`
        // shrinks from the right — which is exactly why the cell is the
        // monitor's LABEL (`…vmss000001`, the tail that differs) and not
        // the raw name: real node names differ only in their tail, and two
        // different nodes truncated to `aks-sigxacto…` would read as one,
        // the inverse of the finding. The same label repeated down the
        // column is still the finding; the full name is in the drill-down.
        key: 'node',
        header: 'NODE',
        value: (s) => (s.meta?.node ? (labels.get(s.meta.node) ?? s.meta.node) : '—')
    }
];

/**
 * Colour the selected row, on top of whatever its own state says.
 *
 * `DataTable` draws a `❯` for its cursor, but only while it HAS focus — and
 * in a shell whose command input is focusable, this table generally does
 * not. The viewport still follows the cursor (that is what the model
 * buys), so without this the selection would be visible only as "the rows
 * scrolled", which is not a selection anyone can see. The row's own status
 * stays legible in its STATUS column.
 */
function selectionTone<T>(
    rows: readonly T[],
    cursor: Model<number> | undefined,
    fallback: (row: T) => string | undefined
): (row: T) => string | undefined {
    return (row: T) => {
        const index = cursor?.value ?? -1;
        if (index >= 0 && rows[index] === row) return 'accent';
        return fallback(row);
    };
}

export function HostsScreen(props: { state: DashboardState; cursor?: Model<number>; pane?: Pane }) {
    const pane = props.pane ?? DEFAULT_PANE;
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <Line color="dim">connecting…</Line>;
    const unreachable = (snapshot.cluster?.unreachable ?? []).map(
        (failure) => `  ${failure.hostId}  ${failure.address}  ${failure.reason} — ${failure.message}`
    );
    const spent = alertHeight(props.state, pane) + blockHeight(unreachable);
    return (
        <Col>
            {alerts(props.state, pane)}
            <DataTable
                columns={hostColumns(nodeLabels(snapshot.hosts))}
                rows={snapshot.hosts}
                model={props.cursor}
                width={pane.width - TABLE_GUTTER}
                height={tableRows(pane, spent)}
                identity={(host: HostView) => host.hostId}
                tone={selectionTone(snapshot.hosts, props.cursor, (host: HostView) => hostColor(host.status))}
                emptyText="no hosts"
            />
            {block('unreachable', unreachable, pane, 'danger')}
        </Col>
    );
}

const grainColumns: TableColumn<ActivationInfo>[] = [
    { key: 'type', header: 'TYPE', value: (g) => g.type },
    // Actor keys are user data and open-ended, so this is the column that
    // matters most and the one `DataTable` protects: it shrinks from the
    // RIGHT, so `KEPT` and `TASKS` give up cells before an identity does.
    { key: 'key', header: 'KEY', value: (g) => g.key, min: 10 },
    { key: 'queued', header: 'QUEUE', value: (g) => String(g.queued), align: 'right' },
    { key: 'age', header: 'AGE', value: (g) => uptime(g.ageMs), align: 'right' },
    { key: 'idle', header: 'IDLE', value: (g) => uptime(g.idleMs), align: 'right' },
    { key: 'tasks', header: 'TASKS', value: (g) => (g.tasks > 0 ? String(g.tasks) : ''), align: 'right' },
    { key: 'alive', header: 'KEPT', value: (g) => (g.keptAlive ? 'yes' : '') }
];

export function GrainsScreen(props: { state: DashboardState; cursor?: Model<number>; pane?: Pane }) {
    const pane = props.pane ?? DEFAULT_PANE;
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <Line color="dim">connecting…</Line>;
    const metrics = snapshot.metrics;
    const actors = snapshot.activations ?? [];
    const slowest = metrics
        ? Object.entries(metrics.byMethod)
              .sort((a, b) => (b[1].turnMs?.p99Ms ?? 0) - (a[1].turnMs?.p99Ms ?? 0))
              .slice(0, 8)
              .map(
                  ([name, m]) =>
                      `  ${name.padEnd(30)} ${durationMs(m.turnMs?.p99Ms ?? 0).padStart(9)}  ` +
                      `${count(m.calls)} calls${m.failed > 0 ? `  ${count(m.failed)} failed` : ''}`
              )
        : [];
    const spent = alertHeight(props.state, pane) + blockHeight(slowest);

    return (
        <Col>
            {alerts(props.state, pane)}
            {/* The actor list comes from the host being POLLED, not from the
                fan-out — so it says so, rather than sitting unlabelled under
                a screen of cluster totals. */}
            <Heading color="dim">
                {fitCell(`actors on ${polledLabel(props.state.view)}`, pane.width)}
            </Heading>
            {snapshot.activations ? (
                <DataTable
                    columns={grainColumns}
                    rows={actors}
                    model={props.cursor}
                    width={pane.width - TABLE_GUTTER}
                    height={tableRows(pane, spent)}
                    identity={(actor: ActivationInfo) => `${actor.type}/${actor.key}`}
                    tone={selectionTone(actors, props.cursor, (actor: ActivationInfo) =>
                        actor.queued > 0 ? 'warn' : undefined
                    )}
                    emptyText="no live activations"
                />
            ) : (
                <Line color="dim">no activation list — the source reports none</Line>
            )}
            {block('slowest methods (p99 turn)', slowest, pane)}
        </Col>
    );
}

/**
 * One host, in full — the drill-down the Hosts cursor now opens.
 *
 * Before this, selecting a row did nothing: the cursor moved and the screen
 * did not change, because a `HostReport` carried no metrics, no health and
 * no actors to show. It carries all three now, so this panel is per-host
 * all the way down — and says so at the top, since nothing on it is a
 * cluster total.
 */
export function HostScreen(props: { state: DashboardState; hostId: string; pane?: Pane }) {
    const pane = props.pane ?? DEFAULT_PANE;
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <Line color="dim">connecting…</Line>;
    const host = snapshot.hosts.find((candidate) => candidate.hostId === props.hostId);
    if (!host) {
        // It was in the last view and is not in this one. That is a fact
        // worth stating, not an empty panel.
        return (
            <Col>
                {alerts(props.state, pane)}
                <Line color="warn">
                    {fitCell(`${props.hostId} is no longer in the membership view`, pane.width)}
                </Line>
                <Line color="dim">esc — back to the host list</Line>
            </Col>
        );
    }

    const digest = host.metrics;
    const latency = digest?.latency ? digestSnapshot(digest.latency) : null;
    const failed = digest?.calls.failed ?? 0;
    const checks = host.health
        ? Object.entries(host.health.checks).map(
              ([name, check]) =>
                  `  ${check.ready ? 'ok  ' : 'FAIL'} ${name}${check.detail ? ` — ${check.detail}` : ''}`
          )
        : [];
    const kinds = digest
        ? Object.entries(digest.errors.byKind)
              .sort((a, b) => (b[1] ?? 0) - (a[1] ?? 0))
              .map(([kind, n]) => `  ${kind.padEnd(20)} ${count(n ?? 0)}`)
        : [];
    const recent = (digest?.errors.recent ?? []).map(
        (entry) =>
            `  ${new Date(entry.at).toISOString().slice(11, 19)} ${entry.type}#${entry.method} ` +
            `${entry.kind}: ${entry.message}`
    );
    const actors = host.activations ?? [];
    const node = host.meta?.node;
    const spent =
        alertHeight(props.state, pane) +
        1 +
        (host.health ? 4 : 3) +
        (node ? 1 : 0) +
        (digest ? 3 : 1) +
        blockHeight(checks) +
        blockHeight(kinds) +
        blockHeight(recent) +
        2;

    return (
        <Col>
            {alerts(props.state, pane)}
            <Heading color="accent">{fitCell(`host ${host.hostId}`, pane.width)}</Heading>
            <DetailList
                labelWidth={LABEL_WIDTH}
                rows={[
                    { label: 'status', value: host.status, tone: hostColor(host.status) },
                    { label: 'address', value: host.address },
                    ...(node ? [{ label: 'node', value: node }] : []),
                    { label: 'up', value: uptime(host.uptimeMs) },
                    ...(host.health
                        ? [
                              {
                                  label: 'ready',
                                  value: host.health.fatal
                                      ? 'FATAL'
                                      : host.health.ready
                                        ? 'yes'
                                        : 'NO',
                                  tone: host.health.fatal
                                      ? 'danger'
                                      : host.health.ready
                                        ? 'success'
                                        : 'warn'
                              }
                          ]
                        : [])
                ]}
            />
            {digest ? (
                <DetailList
                    labelWidth={LABEL_WIDTH}
                    rows={[
                        {
                            label: 'calls',
                            value:
                                `${count(digest.calls.total)}  ${count(failed)} failed (${percent(failed, digest.calls.total)})` +
                                ((digest.calls.oneWayFailures ?? 0) > 0
                                    ? `  ${count(digest.calls.oneWayFailures ?? 0)} one-way`
                                    : ''),
                            tone:
                                failed > 0 || (digest.calls.oneWayFailures ?? 0) > 0
                                    ? 'warn'
                                    : undefined
                        },
                        {
                            label: 'latency',
                            value: latency
                                ? `p50 ${durationMs(latency.p50Ms)}  p99 ${durationMs(latency.p99Ms)}`
                                : 'no samples'
                        },
                        {
                            label: 'storage',
                            value: `${count(digest.storage.loads)} loads  ${count(digest.storage.saves)} saves  ${count(digest.storage.conflicts)} conflicts`
                        }
                    ]}
                />
            ) : (
                <Line color="dim">no metrics from this host</Line>
            )}
            {block('checks', checks, pane)}
            {block('errors by kind', kinds, pane)}
            {block('recent failures', recent, pane)}
            <br />
            <Heading color="dim">actors on this host</Heading>
            {host.activations ? (
                <DataTable
                    columns={grainColumns}
                    rows={actors}
                    width={pane.width - TABLE_GUTTER}
                    height={tableRows(pane, spent)}
                    identity={(actor: ActivationInfo) => `${actor.type}/${actor.key}`}
                    tone={(actor: ActivationInfo) => (actor.queued > 0 ? 'warn' : undefined)}
                    emptyText="no live activations"
                />
            ) : (
                <Line color="dim">  waiting for a detail poll…</Line>
            )}
        </Col>
    );
}

export function ClusterScreen(props: { state: DashboardState; pane?: Pane }) {
    const pane = props.pane ?? DEFAULT_PANE;
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <Line color="dim">connecting…</Line>;
    const cluster = snapshot.cluster;
    if (!cluster) {
        return (
            <Col>
                {alerts(props.state, pane)}
                <Line color="dim">single-node — no cluster to report on</Line>
            </Col>
        );
    }
    const c = cluster.totals.counters;
    const cacheTotal = c.routeCacheHits + c.routeCacheMisses;
    // `?? 0`: a fleet still on a build that predates the pair (#52) reports
    // neither field, and `NaN%` is not an answer.
    const dispatchesLocal = c.dispatchesLocal ?? 0;
    const dispatches = dispatchesLocal + (c.dispatchesRemote ?? 0);
    // Each cell is a 4-wide label, a glyph and a space — six columns — so
    // a 60-column pane holds the full eight per row and 16 shards stay on
    // two lines, which is the line the `locality` header row (#52) needs
    // inside a 20-row pane. Narrower than 48 the grid folds rather than
    // running off the edge.
    const perRow = Math.max(4, Math.min(8, Math.floor(pane.width / 6)));
    // Two columns of counters rather than one long list: fifteen rows plus a
    // shard grid does not fit a 20-row pane, and bare counters are exactly
    // the content that still reads at half width. Anything with a COMPOUND
    // value stays in the full-width header, where it has room.
    const columnWidth = Math.floor((pane.width - 2) / 2);
    const counterLabel = Math.max(8, Math.min(20, columnWidth - 8));

    return (
        <Col>
            {alerts(props.state, pane)}
            <Heading color="dim">{fitCell(scopeOf(snapshot), pane.width)}</Heading>
            <DetailList
                labelWidth={Math.min(20, pane.width - 12)}
                rows={[
                    { label: 'view', value: `#${cluster.view.version}  ${cluster.view.active} active of ${cluster.view.size}` },
                    { label: 'collected from', value: cluster.from },
                    {
                        label: 'route cache',
                        value: `${percent(c.routeCacheHits, cacheTotal)} hit  (${count(cacheTotal)} lookups)`
                    },
                    {
                        // `locateRemote / locates` is the miss rate the edge
                        // is producing: high and staying high means whatever
                        // routes in front of the cluster is not agreeing
                        // with placement.
                        label: 'locates',
                        value: `${count(c.locates)}  ${percent(c.locateRemote, c.locates)} answered "a peer owns it"`
                    },
                    {
                        // The per-request locality fraction (#52). Read
                        // this, not `routedLocal`: that one counts placement
                        // decisions and never sees the warm local fast path.
                        label: 'locality',
                        value: `${percent(dispatchesLocal, dispatches)} local  (${count(dispatches)} dispatches)`
                    }
                ]}
            />
            <Row gap={2}>
                <Col>
                    <DetailList
                        labelWidth={counterLabel}
                        rows={[
                            // Side by side and NEVER summed: the gap between
                            // them is itself the signal.
                            { label: 'remoteDispatches', value: count(c.remoteDispatches) },
                            { label: 'inboundDispatches', value: count(c.inboundDispatches) },
                            { label: 'routedLocal', value: count(c.routedLocal) },
                            // A watch holds a keep-alive on the owner until
                            // the subscriber leaves, so this is the number
                            // worth reading next to an activation count.
                            { label: 'remoteWatches', value: count(c.remoteWatches) },
                            { label: 'inboundWatches', value: count(c.inboundWatches) },
                            { label: 'retries', value: count(c.retries), tone: c.retries > 0 ? 'warn' : undefined },
                            { label: 'routingFailures', value: count(c.routingFailures), tone: c.routingFailures > 0 ? 'danger' : undefined }
                        ]}
                    />
                </Col>
                <Col>
                    <DetailList
                        labelWidth={counterLabel}
                        rows={[
                            { label: 'directoryClaims', value: count(c.directoryClaims) },
                            { label: 'claimConflicts', value: count(c.claimConflicts), tone: c.claimConflicts > 0 ? 'warn' : undefined },
                            { label: 'wrongHostRedirects', value: count(c.wrongHostRedirects) },
                            { label: 'unreachableRetries', value: count(c.unreachableRetries), tone: c.unreachableRetries > 0 ? 'warn' : undefined },
                            { label: 'transportFallbacks', value: count(c.transportFallbacks), tone: c.transportFallbacks > 0 ? 'warn' : undefined },
                            { label: 'authFailures', value: count(c.authFailures), tone: c.authFailures > 0 ? 'danger' : undefined },
                            { label: 'selfFences', value: count(c.selfFences), tone: c.selfFences > 0 ? 'danger' : undefined }
                        ]}
                    />
                </Col>
            </Row>
            <br />
            <StatusGrid
                cells={shardCells(cluster.reminderShards)}
                perRow={perRow}
                legend={fitCell('reminder shards ● claimed ○ UNCLAIMED ◆ claimed twice', pane.width)}
                emptyText="no reminder shards reported"
            />
        </Col>
    );
}

export function HealthScreen(props: { state: DashboardState; pane?: Pane }) {
    const pane = props.pane ?? DEFAULT_PANE;
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <Line color="dim">connecting…</Line>;
    const health = snapshot.health;
    const metrics = snapshot.metrics;

    const checks = health
        ? Object.entries(health.checks).map(
              ([name, check]) =>
                  `  ${check.ready ? 'ok  ' : 'FAIL'} ${name}${check.detail ? ` — ${check.detail}` : ''}`
          )
        : [];
    const checkLines = checks.length > 0 ? checks : ['  no readiness checks contributed'];
    const kinds = metrics
        ? Object.entries(metrics.errors.byKind)
              .sort((a, b) => (b[1] ?? 0) - (a[1] ?? 0))
              .map(([kind, n]) => `  ${kind.padEnd(20)} ${count(n ?? 0)}`)
        : [];
    const kindLines = metrics ? (kinds.length > 0 ? kinds : ['  none']) : [];
    const storage = metrics
        ? [
              `  ${count(metrics.storage.loads)} loads  ${count(metrics.storage.saves)} saves  ${count(metrics.storage.clears)} clears`,
              `  ${count(metrics.storage.conflicts)} etag conflicts — each one discarded an activation`
          ]
        : [];

    // The banner under the gauges: `fatal` and drain are mutually exclusive.
    const verdict = health?.fatal
        ? 'FATAL — this host cannot recover; replace it, draining will not help'
        : health && health.live && !health.ready
          ? 'ALIVE but out of rotation — drain it, do not restart it'
          : null;

    // Recent failures are the first block to give up room when the pane is
    // small: it is the only one here that is a log rather than a status, so
    // a shortened one still says everything the others do.
    const spent =
        alertHeight(props.state, pane) +
        (health ? 3 + (verdict ? 1 : 0) : 1) +
        blockHeight(checkLines) +
        blockHeight(kindLines) +
        blockHeight(storage);
    const room = Math.max(0, pane.height - spent - 2);
    const recent =
        metrics && room > 0
            ? metrics.errors.recent
                  .slice(-Math.min(6, room))
                  .reverse()
                  .map(
                      (entry) =>
                          `  ${new Date(entry.at).toISOString().slice(11, 19)} ${entry.type}#${entry.method} ` +
                          `${entry.kind}: ${entry.message}`
                  )
            : [];

    return (
        <Col>
            {alerts(props.state, pane)}
            {/* Health here is the POLLED host's. Every host's readiness is
                on the Hosts tab and in its drill-down — this one used to be
                the only one visible, which is how a fleet with a fenced
                peer read as healthy. */}
            <Heading color="dim">{fitCell(polledLabel(props.state.view), pane.width)}</Heading>
            {health
                ? [
                      <DetailList
                          labelWidth={LABEL_WIDTH}
                          rows={[
                              // `fatal` is not "very not ready": it says this
                              // host identity is unrecoverable, so liveness
                              // fails and the pod is meant to be REPLACED.
                              // Without it, a fenced host reads as merely
                              // draining and sits there forever.
                              {
                                  label: 'live',
                                  value: health.fatal ? 'FATAL' : health.live ? 'yes' : 'no',
                                  tone: health.fatal || !health.live ? 'danger' : 'success'
                              },
                              { label: 'ready', value: health.ready ? 'yes' : 'NO', tone: health.ready ? 'success' : 'warn' },
                              { label: 'uptime', value: uptime(health.uptimeMs) }
                          ]}
                      />,
                      ...(verdict
                          ? [
                                <Line color={health.fatal ? 'danger' : 'warn'} bold>
                                    {fitCell(verdict, pane.width)}
                                </Line>
                            ]
                          : [])
                  ]
                : [<Line color="dim">no health status — export an ops() or health() handle</Line>]}
            {block('checks', checkLines, pane)}
            {block('errors by kind', kindLines, pane)}
            {block('recent failures', recent, pane)}
            {block('storage', storage, pane)}
        </Col>
    );
}

/**
 * How many rows a table may draw: the pane, less what the rest of the
 * screen has already spent and the chrome the table draws around itself.
 *
 * Floored rather than allowed to go negative — a cramped pane should show a
 * short table, not an inverted one.
 */
function tableRows(pane: Pane, spent: number): number {
    return Math.max(MIN_TABLE_ROWS, pane.height - spent - TABLE_CHROME);
}

function sum<T>(items: readonly T[], of: (item: T) => number): number {
    let total = 0;
    for (const item of items) total += of(item);
    return total;
}
