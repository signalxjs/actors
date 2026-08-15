/** @jsxImportSource @sigx/runtime-core */
/**
 * The pieces every panel is built from.
 *
 * Deliberately small and dumb: each takes already-decided data and draws it.
 * Nothing here decides what a number MEANS — that is
 * `@sigx/actors-monitor`'s, and the whole point of the layering is that this
 * file cannot quietly disagree with the terminal about it.
 *
 * None of the terminal's layout arithmetic carries over. `tableRows`,
 * `blockHeight`, `fitCell`, `wrapText` and `TABLE_CHROME` exist because a
 * terminal has a fixed cell grid and its shell keeps the TOP when a screen
 * overflows, so the last rows silently vanish. A browser has `overflow`, and
 * these components take no pane.
 */
import type { Alert, PercentilePoint, Rate, ShardStatus } from '@sigx/actors-monitor';

/** A severity, as this dashboard's CSS spells it. */
export type Tone = 'ok' | 'warn' | 'danger' | 'dim';

/** `undefined` for "no tone" — a class list must not gain a bare `undefined`. */
export const toneClass = (tone: Tone | null | undefined): string | undefined =>
    tone ? `sxad-${tone}` : undefined;

/* -- alerts ----------------------------------------------------------- */

/**
 * The banner. Empty renders nothing at all rather than an empty box, so a
 * healthy dashboard does not carry a permanent blank strip.
 *
 * `role="alert"` and `aria-live` are on the container rather than on each
 * line: a screen reader should hear "3 shards unclaimed" once when it
 * appears, not on every poll that leaves it unchanged.
 */
export function Alerts(props: { alerts: readonly Alert[] }) {
    if (props.alerts.length === 0) return null;
    return (
        <div class="sxad-alerts" role="alert" aria-live="polite">
            {props.alerts.map((alert) => (
                <div class={`sxad-alert sxad-${alert.tone}`}>{alert.text}</div>
            ))}
        </div>
    );
}

/**
 * The pre-first-snapshot state: the alert banner, then what we are waiting on.
 *
 * A component rather than a line in each panel, because the ORDER is the fix
 * — alerts above, always. Six panels each writing `if (!snapshot) return …`
 * is six chances to put the early return above the banner again, which is the
 * bug this replaces (#256).
 */
export function Awaiting(props: { alerts: readonly Alert[]; message: string; failed: boolean }) {
    return (
        <div>
            <Alerts alerts={props.alerts} />
            <p class={props.failed ? 'sxad-note' : 'sxad-empty'}>{props.message}</p>
        </div>
    );
}

/* -- definition rows --------------------------------------------------- */

export interface DetailRow {
    label: string;
    value: string;
    tone?: Tone | null;
}

/** Label/value pairs, as a real `<dl>` so the pairing survives a screen reader. */
export function DetailList(props: { rows: readonly DetailRow[] }) {
    return (
        <dl class="sxad-list">
            {props.rows.map((row) => [
                <dt>{row.label}</dt>,
                <dd class={toneClass(row.tone)}>{row.value}</dd>
            ])}
        </dl>
    );
}

/* -- tables ------------------------------------------------------------ */

export interface Column<T> {
    key: string;
    header: string;
    value: (row: T) => string;
    /** Right-aligned and tabular — for anything you would compare down a column. */
    numeric?: boolean;
    /**
     * Monospaced and ellipsised. For actor keys, which are user data and
     * open-ended: one UUID key would otherwise stretch the table past the
     * viewport and push every number out of sight.
     */
    key_?: boolean;
}

/**
 * A table, in an `overflow-x` wrapper.
 *
 * The wrapper is the browser's whole answer to what cost the terminal three
 * paragraphs of budgeting: a table too wide scrolls, rather than clipping
 * columns from the right.
 *
 * **`onPick` puts a real `<button>` in the first cell**, and does not merely
 * hang a click handler on the `<tr>`. A clickable row is invisible to the
 * keyboard and announced as a plain row by a screen reader, so the drill-down
 * would simply not exist for anyone not using a mouse — the same class of
 * failure as re-rendering the tab strip on every poll, and worse, because
 * there is no visible symptom at all. The row keeps a click handler as a
 * convenience for the mouse; the button is what makes the action real.
 */
export function DataTable<T>(props: {
    columns: readonly Column<T>[];
    rows: readonly T[];
    /** Row tone — status, queue depth, selection. */
    tone?: (row: T) => Tone | null | undefined;
    /** Makes rows pickable; the drill-down uses it. */
    onPick?: (row: T) => void;
    /** What activating a row does, for the accessible name: "open host a1". */
    pickLabel?: (row: T) => string;
    emptyText: string;
    caption?: string;
}) {
    if (props.rows.length === 0) return <p class="sxad-empty">{props.emptyText}</p>;
    const pick = props.onPick;
    const first = props.columns[0]?.key;
    return (
        <div class="sxad-tablewrap">
            <table class="sxad-table">
                {props.caption ? <caption class="sxad-scope">{props.caption}</caption> : null}
                <thead>
                    <tr>
                        {props.columns.map((column) => (
                            <th scope="col" class={column.numeric ? 'sxad-num' : undefined}>
                                {column.header}
                            </th>
                        ))}
                    </tr>
                </thead>
                <tbody>
                    {props.rows.map((row) => (
                        <tr
                            class={toneClass(props.tone?.(row))}
                            data-sxad-click={pick ? '' : undefined}
                            onClick={pick ? () => pick(row) : undefined}
                        >
                            {props.columns.map((column) => {
                                const text = column.value(row);
                                // The button goes in the IDENTIFYING cell, so
                                // its accessible name is the thing you are
                                // opening rather than "open" repeated N times.
                                const actionable = pick && column.key === first;
                                return (
                                    <td
                                        class={
                                            column.numeric
                                                ? 'sxad-num'
                                                : column.key_
                                                  ? 'sxad-key'
                                                  : undefined
                                        }
                                        title={column.key_ ? text : undefined}
                                    >
                                        {actionable ? (
                                            <button
                                                type="button"
                                                class="sxad-rowbtn"
                                                aria-label={props.pickLabel?.(row)}
                                                onClick={(event: MouseEvent) => {
                                                    // The row handler would
                                                    // otherwise fire too.
                                                    event.stopPropagation();
                                                    pick(row);
                                                }}
                                            >
                                                {text}
                                            </button>
                                        ) : (
                                            text
                                        )}
                                    </td>
                                );
                            })}
                        </tr>
                    ))}
                </tbody>
            </table>
        </div>
    );
}

/* -- sparkline --------------------------------------------------------- */

/** How tall the SVG's own coordinate space is. Purely internal. */
const SPARK_H = 20;

/**
 * One series as an SVG polyline, scaled from ZERO.
 *
 * Two decisions carried over from the terminal, both because the alternative
 * misleads:
 *
 *   - **Zero-anchored, not min-anchored.** A min-anchored line turns a flat
 *     series at 1000 into a mountain range of noise.
 *   - **A gap BREAKS the line.** `null` means the counter reset, or the poll
 *     failed, or a peer left the fan-out — and joining across it draws a
 *     slope that reads as real change. Each run of readings becomes its own
 *     `M…L…` subpath, so the break is visible as a break.
 */
export function Sparkline(props: { values: readonly Rate[]; tone?: Tone }) {
    const values = props.values;
    const width = Math.max(values.length - 1, 1);
    let peak = 0;
    for (const value of values) {
        if (value !== null && value > peak) peak = value;
    }
    // A flat-zero series still draws a line along the baseline — "we
    // measured, and it was zero" is a real reading and not a gap.
    const scale = peak > 0 ? peak : 1;

    const segments: string[] = [];
    let current: string[] = [];
    values.forEach((value, index) => {
        if (value === null) {
            if (current.length > 0) segments.push(current.join(' '));
            current = [];
            return;
        }
        const x = (index / width) * 100;
        const y = SPARK_H - (value / scale) * SPARK_H;
        current.push(`${current.length === 0 ? 'M' : 'L'}${x.toFixed(2)} ${y.toFixed(2)}`);
    });
    if (current.length > 0) segments.push(current.join(' '));

    return (
        <svg
            class="sxad-spark"
            viewBox={`0 0 100 ${SPARK_H}`}
            preserveAspectRatio="none"
            aria-hidden="true"
        >
            {segments.map((d) => (
                // A single reading between two gaps has no length, so it
                // would draw nothing without a round cap.
                <path d={d} stroke="currentColor" stroke-linecap="round" />
            ))}
        </svg>
    );
}

/**
 * A labelled series: name, sparkline, current value.
 *
 * The value is a STRING the caller formatted, because a rate and a gauge are
 * not the same quantity — `33/s` under an activation count claims a
 * throughput nobody measured — and choosing between them is the panel's job.
 */
export function Series(props: {
    label: string;
    values: readonly Rate[];
    value: string;
    tone?: Tone;
}) {
    return (
        <div class={`sxad-series ${toneClass(props.tone) ?? ''}`}>
            <span class="sxad-label">{props.label}</span>
            <Sparkline values={props.values} tone={props.tone} />
            <span class="sxad-value">{props.value}</span>
        </div>
    );
}

/* -- percentile bars ---------------------------------------------------- */

/**
 * p50 / p90 / p99 against a scale the CALLER owns.
 *
 * One shared ceiling across latency, queue and turn: the comparison IS the
 * diagnosis (high queue = a hot actor, high turn = a slow method), and
 * auto-scaling each group would draw a 12µs queue wait and a 47ms turn
 * identically.
 *
 * A `null` point draws a hatched EMPTY track, not a zero-width bar. An empty
 * histogram means "no reading"; a flat row of zeroes would assert "we
 * measured, and it was fast".
 */
export function Bars(props: {
    points: readonly PercentilePoint[];
    ceiling: number;
    format: (ms: number) => string;
    tone?: Tone;
    emptyText: string;
}) {
    if (props.points.every((point) => point.value === null)) {
        return <p class="sxad-empty">{props.emptyText}</p>;
    }
    return (
        <div class={`sxad-bars ${toneClass(props.tone) ?? ''}`}>
            {props.points.map((point) => [
                <span class="sxad-label">{point.label}</span>,
                <div class={`sxad-track${point.value === null ? ' sxad-noread' : ''}`}>
                    {point.value === null ? null : (
                        <div
                            class="sxad-fill"
                            style={{
                                width: `${props.ceiling > 0 ? Math.max((point.value / props.ceiling) * 100, 1) : 0}%`
                            }}
                        />
                    )}
                </div>,
                <span class="sxad-value">
                    {point.value === null ? '—' : props.format(point.value)}
                </span>
            ])}
        </div>
    );
}

/* -- reminder shards ------------------------------------------------------ */

/** The three states, as the CSS spells them. */
const SHARD_TONE: Record<ShardStatus['state'], Tone> = {
    claimed: 'ok',
    unclaimed: 'danger',
    split: 'warn'
};

/**
 * The reminder shard map.
 *
 * The legend is not decoration: an operator seeing sixteen coloured pills has
 * no way to know that the red one means *nothing is ticking that shard*,
 * which is the finding this panel exists for.
 */
export function ShardGrid(props: { shards: readonly ShardStatus[]; emptyText: string }) {
    if (props.shards.length === 0) return <p class="sxad-empty">{props.emptyText}</p>;
    return [
        <div class="sxad-shards">
            {props.shards.map((shard) => (
                <span
                    class={`sxad-shard sxad-${SHARD_TONE[shard.state]}`}
                    title={
                        shard.state === 'unclaimed'
                            ? 'UNCLAIMED — nothing is ticking this shard'
                            : shard.claimants.join(' ')
                    }
                >
                    {shard.label}
                </span>
            ))}
        </div>,
        <p class="sxad-legend">
            reminder shards — claimed · <b>UNCLAIMED (nothing is ticking them)</b> · claimed twice
            (views have diverged)
        </p>
    ];
}

/* -- misc ------------------------------------------------------------------ */

/** A titled block that renders nothing when it has nothing to say. */
export function Section(props: { title: string; lines: readonly string[]; tone?: Tone }) {
    if (props.lines.length === 0) return null;
    return (
        <section class="sxad-section">
            <h3>{props.title}</h3>
            <ul class="sxad-log">
                {props.lines.map((line) => (
                    <li class={toneClass(props.tone)}>{line}</li>
                ))}
            </ul>
        </section>
    );
}
