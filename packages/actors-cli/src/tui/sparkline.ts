/**
 * Sparkline — a time series in one line of text.
 *
 * `@sigx/terminal` has `ProgressBar` but nothing time-series, and a
 * dashboard's most useful single glyph is "is this going up?". Intended to
 * upstream to `@sigx/terminal-ui` once it has settled here.
 *
 * Pure: values in, string out. The JSX wrapper is a `<text>` around this,
 * which keeps the interesting part testable without a renderer.
 */

/** Eight levels, so a column carries three bits of shape. */
const BLOCKS = ['▁', '▂', '▃', '▄', '▅', '▆', '▇', '█'] as const;
/**
 * What a GAP renders as.
 *
 * Gaps are not zeroes. A counter reset, an unreachable poll and a genuine
 * idle period are three different facts, and drawing the first two at the
 * baseline claims the third. A mid-height dot reads as "no reading" without
 * pretending to be a value.
 */
const GAP = '·';
/** A series that is entirely one value has no shape to show. */
const FLAT = '▁';

export interface SparklineOptions {
    /**
     * Upper bound of the scale. Defaults to the highest value present.
     *
     * Pass it explicitly to compare two sparklines — auto-scaling makes
     * every series fill the same height, so a graph of 3 req/s looks
     * identical to one of 30,000.
     */
    max?: number;
    /**
     * Render the LAST `width` values, right-aligned.
     *
     * Right, not left: the newest sample is the one being watched, and a
     * left-aligned series would push it off the edge as history grows.
     */
    width?: number;
    /** Pad to `width` with blanks when there is not enough history yet. */
    pad?: boolean;
}

/**
 * Render values as block characters. `null` is a gap.
 *
 * The scale starts at ZERO rather than at the minimum. A min-anchored
 * sparkline turns a flat line at 1000 req/s into a dramatic mountain range
 * of noise, which is the single most common way these mislead.
 */
export function sparkline(values: readonly (number | null)[], options: SparklineOptions = {}): string {
    const width = options.width !== undefined && Number.isFinite(options.width)
        ? Math.max(0, Math.floor(options.width))
        : values.length;
    if (width === 0) return '';

    const visible = values.length > width ? values.slice(values.length - width) : values;
    const max = options.max !== undefined && Number.isFinite(options.max) ? options.max : peakOf(visible);

    let out = '';
    for (const value of visible) {
        if (value === null || !Number.isFinite(value)) {
            out += GAP;
            continue;
        }
        if (max <= 0) {
            // Every reading is zero: a real, flat, true answer.
            out += FLAT;
            continue;
        }
        const ratio = Math.max(0, Math.min(1, value / max));
        // The lowest block is RESERVED for exact zero, and any non-zero
        // value gets at least the second. It costs one level of resolution
        // and buys the distinction that actually matters at a glance:
        // "nothing happened" versus "something did, but barely". Without
        // it, 1 req/s against a scale of 1000 draws exactly like silence.
        const index =
            ratio === 0 ? 0 : Math.max(1, Math.ceil(ratio * (BLOCKS.length - 1)));
        out += BLOCKS[Math.min(BLOCKS.length - 1, index)];
    }

    if (options.pad && out.length < width) out = ' '.repeat(width - out.length) + out;
    return out;
}

/** Highest finite value, or 0. */
export function peakOf(values: readonly (number | null)[]): number {
    let peak = 0;
    for (const value of values) {
        if (value !== null && Number.isFinite(value) && value > peak) peak = value;
    }
    return peak;
}

/**
 * A horizontal meter: `████████░░░░` for a single ratio.
 *
 * `ProgressBar` upstream covers determinate progress; this is the gauge
 * shape — no percentage, sized to sit inside a table cell.
 */
export function meter(value: number, max: number, width = 12): string {
    const w = Math.max(0, Math.floor(width));
    if (w === 0) return '';
    if (!Number.isFinite(value) || !Number.isFinite(max) || max <= 0) return '░'.repeat(w);
    const ratio = Math.max(0, Math.min(1, value / max));
    // A non-zero ratio always shows at least one cell: rounding a real 2%
    // down to an empty bar reads as "nothing", which is the wrong answer.
    const filled = ratio === 0 ? 0 : Math.max(1, Math.round(ratio * w));
    return '█'.repeat(filled) + '░'.repeat(w - filled);
}

/**
 * A trend marker against the previous value: `▲`, `▼` or `·`.
 *
 * `null` on either side is a gap, not a direction — comparing across a
 * reset would report a crash that never happened.
 */
export function trend(current: number | null, previous: number | null): '▲' | '▼' | '·' {
    if (current === null || previous === null) return '·';
    if (!Number.isFinite(current) || !Number.isFinite(previous)) return '·';
    if (current > previous) return '▲';
    if (current < previous) return '▼';
    return '·';
}
