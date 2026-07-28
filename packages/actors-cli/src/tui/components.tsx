/** @jsxImportSource @sigx/terminal */
/**
 * The JSX layer over `src/tui/`'s pure functions.
 *
 * Deliberately thin: every component here is a `<text>` or a `<box>` around
 * a string the module next door computed. That is what makes the port to
 * `@sigx/terminal-ui` mostly re-styling, and it is why the tests live
 * against the pure side rather than against rendered output.
 *
 * Colour is always a theme TOKEN — `accent`, `success`, `warn`, `danger`,
 * `dim` — never a hex value, so these inherit whatever theme the hosting
 * shell is running.
 */
import { histogramRow, type HistogramRow } from './bars';
import { meter, sparkline, trend } from './sparkline';
import { fit, layoutTable, type Column } from './table';
import type { HistogramSnapshot } from '@sigx/actors/silo';
import type { ShardCell } from './bars';

/** A time series in one line, with its current value alongside. */
export function Sparkline(props: {
    values: readonly (number | null)[];
    width?: number;
    max?: number;
    label?: string;
    value?: string;
    color?: string;
}) {
    return (
        <text>
            {props.label ? <text color="dim">{fit(props.label, 12)}</text> : null}
            <text color={props.color ?? 'accent'}>
                {sparkline(props.values, { width: props.width ?? 24, max: props.max, pad: true })}
            </text>
            {props.value ? <text> {props.value}</text> : null}
        </text>
    );
}

/** A labelled bar for a single ratio. */
export function Meter(props: {
    label: string;
    value: number;
    max: number;
    width?: number;
    text?: string;
    color?: string;
}) {
    return (
        <text>
            <text color="dim">{fit(props.label, 12)}</text>
            <text color={props.color ?? 'accent'}>
                {meter(props.value, props.max, props.width ?? 16)}
            </text>
            {props.text ? <text> {props.text}</text> : null}
        </text>
    );
}

/**
 * One histogram as three labelled percentile bars.
 *
 * `scaleMs` is required rather than derived, because the whole value of
 * stacking latency, queue and turn is reading them against ONE axis.
 */
export function HistogramBars(props: {
    label: string;
    snapshot: HistogramSnapshot | null;
    scaleMs: number;
    format: (ms: number) => string;
    width?: number;
    color?: string;
}) {
    const row: HistogramRow = histogramRow(props.label, props.snapshot, props.scaleMs);
    if (row.empty) {
        return (
            <text>
                <text color="dim">{fit(props.label, 8)}</text>
                <text color="dim">no samples</text>
            </text>
        );
    }
    const width = props.width ?? 10;
    return (
        <text>
            <text color="dim">{fit(props.label, 8)}</text>
            {row.cells.map((cell) => (
                <text>
                    <text color="dim">{cell.label} </text>
                    <text color={props.color ?? 'accent'}>{meter(cell.ratio, 1, width)}</text>
                    <text> {fit(props.format(cell.ms), 8)}</text>
                </text>
            ))}
        </text>
    );
}

/**
 * A table with a cursor.
 *
 * Upstream's `Table` takes `columns: string[]` / `rows: string[][]` and
 * renders them statically. This one lays out typed rows, marks the selected
 * one, and takes a pre-windowed slice so the caller owns scrolling (the
 * viewport maths is `scrollOffset`, next door).
 */
export function DataTable<T>(props: {
    columns: readonly Column<T>[];
    rows: readonly T[];
    /** Index INTO `rows` of the selected row, or -1 for none. */
    cursor?: number;
    width?: number;
    /** Per-row colour token, e.g. to flag a fenced silo. */
    tone?: (row: T) => string | undefined;
}) {
    const table = layoutTable(props.columns, props.rows, { width: props.width });
    const cursor = props.cursor ?? -1;
    return (
        <box>
            <text color="dim" bold>
                {table.header}
            </text>
            {table.rows.map((line, index) => {
                const selected = index === cursor;
                const tone = props.tone?.(props.rows[index]!);
                return (
                    <text
                        color={selected ? 'accentText' : (tone ?? 'fg')}
                        backgroundColor={selected ? 'selSoft' : undefined}
                    >
                        {selected ? '▸' : ' '}
                        {line}
                    </text>
                );
            })}
            {table.rows.length === 0 ? <text color="dim">nothing to show</text> : null}
        </box>
    );
}

/** The reminder-shard grid, coloured by claimant count. */
export function ShardGrid(props: { rows: readonly (readonly ShardCell[])[] }) {
    const toneOf = (cell: ShardCell): string =>
        cell.tone === 'ok' ? 'success' : cell.tone === 'unclaimed' ? 'danger' : 'warn';
    return (
        <box>
            <text color="dim">reminder shards ● claimed ○ UNCLAIMED ◆ claimed twice</text>
            {props.rows.map((row) => (
                <text>
                    {row.map((cell) => (
                        <text>
                            <text color="dim">{fit(cell.shard, 4)}</text>
                            <text color={toneOf(cell)}>{cell.glyph} </text>
                        </text>
                    ))}
                </text>
            ))}
        </box>
    );
}

/** Aligned label/value pairs. */
export function KeyValue(props: {
    rows: readonly { label: string; value: string; tone?: string }[];
    labelWidth?: number;
}) {
    const width = props.labelWidth ?? 14;
    return (
        <box>
            {props.rows.map((row) => (
                <text>
                    <text color="dim">{fit(row.label, width)}</text>
                    <text color={row.tone ?? 'fg'}>{row.value}</text>
                </text>
            ))}
        </box>
    );
}

/** A value with its direction of travel against the previous reading. */
export function DeltaText(props: { value: string; current: number | null; previous: number | null }) {
    const direction = trend(props.current, props.previous);
    const tone = direction === '▲' ? 'warn' : direction === '▼' ? 'success' : 'dim';
    return (
        <text>
            <text>{props.value}</text>
            <text color={tone}> {direction}</text>
        </text>
    );
}
