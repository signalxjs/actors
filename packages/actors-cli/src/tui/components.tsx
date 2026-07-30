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

/**
 * One standalone line.
 *
 * `<text>` is a SPAN — terminal-zero calls it "deliberately INLINE, unlike
 * every other component" — so consecutive `<text>` siblings share a line.
 * That is a feature when composing within a line and a trap when writing a
 * list of rows, and it is how every screen once rendered as a single
 * concatenated line. `Line` is the block wrapper, named so the intent is
 * visible at the call site.
 */
export function Line(props: { color?: string; bold?: boolean; children?: unknown }) {
    return (
        <box>
            <text color={props.color} bold={props.bold}>
                {props.children as never}
            </text>
        </box>
    );
}

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
        <box>
            {props.label ? <text color="dim">{fit(props.label, 12)}</text> : null}
            <text color={props.color ?? 'accent'}>
                {sparkline(props.values, { width: props.width ?? 24, max: props.max, pad: true })}
            </text>
            {props.value ? <text> {props.value}</text> : null}
        </box>
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
        <box>
            <text color="dim">{fit(props.label, 12)}</text>
            <text color={props.color ?? 'accent'}>
                {meter(props.value, props.max, props.width ?? 16)}
            </text>
            {props.text ? <text> {props.text}</text> : null}
        </box>
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
            <box>
                <text color="dim">{fit(props.label, 8)}</text>
                <text color="dim">no samples</text>
            </box>
        );
    }
    const width = props.width ?? 10;
    return (
        // One block row; the percentile cells inside are spans, so they
        // correctly stay on that single line.
        <box>
            <text color="dim">{fit(props.label, 8)}</text>
            {row.cells.map((cell) => (
                <text>
                    <text color="dim">{cell.label} </text>
                    <text color={props.color ?? 'accent'}>{meter(cell.ratio, 1, width)}</text>
                    <text> {fit(props.format(cell.ms), 8)}</text>
                </text>
            ))}
        </box>
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
            <box>
                {/* Indented by the cursor marker's width, or the header
                    sits one column left of every value beneath it. */}
                <text color="dim" bold>
                    {` ${table.header}`}
                </text>
            </box>
            {table.rows.map((line, index) => {
                const selected = index === cursor;
                const tone = props.tone?.(props.rows[index]!);
                return (
                    <box>
                        <text
                            color={selected ? 'accentText' : (tone ?? 'fg')}
                            backgroundColor={selected ? 'selSoft' : undefined}
                        >
                            {selected ? '▸' : ' '}
                            {line}
                        </text>
                    </box>
                );
            })}
            {table.rows.length === 0 ? (
                <box>
                    <text color="dim">nothing to show</text>
                </box>
            ) : null}
        </box>
    );
}

/** The reminder-shard grid, coloured by claimant count. */
export function ShardGrid(props: { rows: readonly (readonly ShardCell[])[] }) {
    const toneOf = (cell: ShardCell): string =>
        cell.tone === 'ok' ? 'success' : cell.tone === 'unclaimed' ? 'danger' : 'warn';
    return (
        <box>
            <box>
                <text color="dim">reminder shards ● claimed ○ UNCLAIMED ◆ claimed twice</text>
            </box>
            {props.rows.map((row) => (
                <box>
                    {row.map((cell) => (
                        <text>
                            <text color="dim">{fit(cell.shard, 4)}</text>
                            <text color={toneOf(cell)}>{cell.glyph} </text>
                        </text>
                    ))}
                </box>
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
                // A <box> is BLOCK; a <text> is a span. Wrapping each pair
                // is what puts it on its own line — sibling <text> elements
                // share one, which is how every row ended up concatenated.
                <box>
                    <text color="dim">{fit(row.label, width)}</text>
                    <text color={row.tone ?? 'fg'}>{row.value}</text>
                </box>
            ))}
        </box>
    );
}

/**
 * A value with its direction of travel against the previous reading.
 *
 * `polarity` decides what rising MEANS. Hardcoding `▲` as a warning is
 * right for latency and wrong for throughput — rising calls/s is good news,
 * and colouring it as a warning makes the component lie half the time.
 * `'lower-is-better'` (the default, for latency, queue depth, errors) or
 * `'higher-is-better'` (throughput, cache hit rate); `'neutral'` just shows
 * the arrow.
 */
export function DeltaText(props: {
    value: string;
    current: number | null;
    previous: number | null;
    polarity?: 'lower-is-better' | 'higher-is-better' | 'neutral';
}) {
    const direction = trend(props.current, props.previous);
    const polarity = props.polarity ?? 'lower-is-better';
    const good = polarity === 'higher-is-better' ? '▲' : '▼';
    const tone =
        direction === '·' || polarity === 'neutral'
            ? 'dim'
            : direction === good
              ? 'success'
              : 'warn';
    return (
        <box>
            <text>{props.value}</text>
            <text color={tone}> {direction}</text>
        </box>
    );
}
