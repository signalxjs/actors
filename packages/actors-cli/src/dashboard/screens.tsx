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
import type { ActivationInfo } from '@sigx/actors/silo';
import { count, durationMs, gauge, percent, rate, uptime } from '../model/format';
import { histogramScale, percentileItems, shardCells, splitShards, unclaimedShards } from '../tui/bars';
import { wrapText } from '../tui/wrap';
import { Line } from '../tui/components';
import type { SiloView } from '../source/types';
import type { DashboardState } from './state';

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

/** Status tones. `fenced` and `leaving` must never look like `active`. */
function siloTone(status: string): string | undefined {
    if (status === 'active' || status === 'unknown') return undefined;
    if (status === 'fenced') return 'danger';
    if (status === 'leaving') return 'warn';
    return 'dim';
}

interface Alert {
    text: string;
    tone: string;
}

/**
 * Everything currently wrong, worst first.
 *
 * Exported because the screens have to BUDGET for these lines as well as
 * draw them — a banner that pushes the table past the bottom of the pane is
 * the same bug as a table that does not know the width.
 */
export function alertLines(state: DashboardState): Alert[] {
    const { snapshot, error, paused } = state.view;
    const lines: Alert[] = [];

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
    return lines;
}

/** Each alert wrapped to the pane, continuation lines indented under it. */
function wrappedAlerts(state: DashboardState, pane: Pane): Alert[] {
    return alertLines(state).flatMap((alert) =>
        wrapText(`! ${alert.text}`, pane.width, '  ').map((text) => ({ text, tone: alert.tone }))
    );
}

/** A banner for whatever is currently wrong, plus a blank — or nothing. */
function alerts(state: DashboardState, pane: Pane): unknown[] {
    const lines = wrappedAlerts(state, pane);
    if (lines.length === 0) return [];
    return [
        ...lines.map((line) => (
            <Line color={line.tone} bold>
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

function totalCounter(silos: readonly SiloView[], key: 'authFailures'): number {
    let total = 0;
    for (const silo of silos) total += silo.counters?.[key] ?? 0;
    return total;
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

export function OverviewScreen(props: { state: DashboardState; pane?: Pane }) {
    const state = props.state;
    const pane = props.pane ?? DEFAULT_PANE;
    const snapshot = state.view.snapshot;
    if (!snapshot) return <Line color="dim">connecting…</Line>;

    const totals = snapshot.cluster?.totals;
    const activations = totals?.activations ?? sum(snapshot.silos, (s) => s.stats.activations);
    const queued = totals?.queued ?? sum(snapshot.silos, (s) => s.stats.queued);
    const metrics = snapshot.metrics;
    const scale = histogramScale([
        metrics?.latencyMs ?? null,
        metrics?.queueMs ?? null,
        metrics?.turnMs ?? null
    ]);

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
            <DetailList
                labelWidth={LABEL_WIDTH}
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
            {metrics
                ? [
                      <br />,
                      /* ONE shared scale across all three: the comparison is
                         the diagnosis, and a per-group scale would draw a
                         12µs queue wait and a 47ms turn identically. */
                      <Row gap={2}>
                          <Col>
                              <Heading color="dim">latency</Heading>
                              {percentiles(metrics.latencyMs, scale, barWidth, 'accent')}
                          </Col>
                          <Col>
                              <Heading color="dim">queue</Heading>
                              {percentiles(metrics.queueMs, scale, barWidth, 'warn')}
                          </Col>
                          <Col>
                              <Heading color="dim">turn</Heading>
                              {percentiles(metrics.turnMs, scale, barWidth, 'accent')}
                          </Col>
                      </Row>,
                      <Line color="dim">
                          {fitCell('high queue = a hot grain · high turn = a slow method', pane.width)}
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

const siloColumns: TableColumn<SiloView>[] = [
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

export function SilosScreen(props: { state: DashboardState; cursor?: Model<number>; pane?: Pane }) {
    const pane = props.pane ?? DEFAULT_PANE;
    const snapshot = props.state.view.snapshot;
    if (!snapshot) return <Line color="dim">connecting…</Line>;
    const unreachable = (snapshot.cluster?.unreachable ?? []).map(
        (failure) => `  ${failure.siloId}  ${failure.address}  ${failure.reason} — ${failure.message}`
    );
    const spent = alertHeight(props.state, pane) + blockHeight(unreachable);
    return (
        <Col>
            {alerts(props.state, pane)}
            <DataTable
                columns={siloColumns}
                rows={snapshot.silos}
                model={props.cursor}
                width={pane.width - TABLE_GUTTER}
                height={tableRows(pane, spent)}
                identity={(silo: SiloView) => silo.siloId}
                tone={selectionTone(snapshot.silos, props.cursor, (silo: SiloView) => siloTone(silo.status))}
                emptyText="no silos"
            />
            {block('unreachable', unreachable, pane, 'danger')}
        </Col>
    );
}

const grainColumns: TableColumn<ActivationInfo>[] = [
    { key: 'type', header: 'TYPE', value: (g) => g.type },
    // Grain keys are user data and open-ended, so this is the column that
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
    const grains = snapshot.activations ?? [];
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
            {snapshot.activations ? (
                <DataTable
                    columns={grainColumns}
                    rows={grains}
                    model={props.cursor}
                    width={pane.width - TABLE_GUTTER}
                    height={tableRows(pane, spent)}
                    identity={(grain: ActivationInfo) => `${grain.type}/${grain.key}`}
                    tone={selectionTone(grains, props.cursor, (grain: ActivationInfo) =>
                        grain.queued > 0 ? 'warn' : undefined
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
    // Each cell is a label, a glyph and a space; 16 shards would otherwise
    // run off a narrow pane.
    const perRow = Math.max(4, Math.min(8, Math.floor(pane.width / 8)));
    // Two columns of counters rather than one long list: fifteen rows plus a
    // shard grid does not fit a 20-row pane, and bare counters are exactly
    // the content that still reads at half width. Anything with a COMPOUND
    // value stays in the full-width header, where it has room.
    const columnWidth = Math.floor((pane.width - 2) / 2);
    const counterLabel = Math.max(8, Math.min(20, columnWidth - 8));

    return (
        <Col>
            {alerts(props.state, pane)}
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
        ? 'FATAL — this silo cannot recover; replace it, draining will not help'
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
            {health
                ? [
                      <DetailList
                          labelWidth={LABEL_WIDTH}
                          rows={[
                              // `fatal` is not "very not ready": it says this
                              // silo identity is unrecoverable, so liveness
                              // fails and the pod is meant to be REPLACED.
                              // Without it, a fenced silo reads as merely
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
