/**
 * Formatting for dense, scannable columns.
 *
 * A dashboard's job is comparison, so the rules here optimise for that
 * rather than for precision: fixed widths, three significant figures, and
 * units that keep a column readable at a glance. No terminal imports — a
 * web dashboard wants the same strings.
 */

/** Counts: 950, 12.4k, 3.1M. */
export function count(value: number): string {
    if (!Number.isFinite(value)) return '—';
    const sign = value < 0 ? '-' : '';
    const n = Math.abs(value);
    if (n < 1000) return `${sign}${Math.round(n)}`;
    if (n < 1_000_000) return `${sign}${trim(n / 1000)}k`;
    if (n < 1_000_000_000) return `${sign}${trim(n / 1_000_000)}M`;
    return `${sign}${trim(n / 1_000_000_000)}B`;
}

/**
 * Durations in ms, chosen so latency columns line up: sub-ms in µs, then
 * ms, then seconds. `p99 812µs` next to `p99 31ms` is instantly comparable
 * in a way `0.812ms` and `31.4ms` are not.
 */
export function durationMs(ms: number): string {
    if (!Number.isFinite(ms)) return '—';
    if (ms < 0.001) return '0';
    if (ms < 1) return `${Math.round(ms * 1000)}µs`;
    if (ms < 1000) return `${trim(ms)}ms`;
    if (ms < 60_000) return `${trim(ms / 1000)}s`;
    return uptime(ms);
}

/** Elapsed wall time: 4h12m, 3d 6h, 45s. */
export function uptime(ms: number): string {
    if (!Number.isFinite(ms) || ms < 0) return '—';
    const seconds = Math.floor(ms / 1000);
    if (seconds < 60) return `${seconds}s`;
    const minutes = Math.floor(seconds / 60);
    if (minutes < 60) return `${minutes}m${pad(seconds % 60)}s`;
    const hours = Math.floor(minutes / 60);
    if (hours < 24) return `${hours}h${pad(minutes % 60)}m`;
    return `${Math.floor(hours / 24)}d ${hours % 24}h`;
}

/** A per-second rate, or an em dash for a gap. */
export function rate(value: number | null): string {
    return value === null ? '—' : `${count(value)}/s`;
}

/**
 * A gauge reading — how many there are right now — or an em dash for a gap.
 *
 * The counterpart to `rate`, and kept distinct from it on purpose: queue
 * depth and activation count are not per-second quantities, and labelling
 * one "33/s" claims a throughput that was never measured.
 */
export function gauge(value: number | null): string {
    return value === null ? '—' : count(value);
}

/** A ratio as a whole percent; `null` when the denominator is 0. */
export function percent(numerator: number, denominator: number): string {
    if (denominator <= 0) return '—';
    return `${Math.round((numerator / denominator) * 100)}%`;
}

/**
 * Truncate to `width` CODE POINTS, marking the cut with an ellipsis.
 *
 * Grain keys are user data and can be long; a table that reflows because
 * one key is a UUID is unreadable, and silently cutting hides that the key
 * continues.
 *
 * Code points rather than code units, so an emoji or an astral-plane
 * character is never cut in half into a replacement glyph. It is NOT
 * display cells: this module is deliberately renderer-free (a web
 * dashboard imports it too), and cell-accurate fitting belongs to whatever
 * is drawing — in the terminal that is `fitCell` from `@sigx/terminal`,
 * which is what every column here actually goes through.
 */
export function ellipsis(text: string, width: number): string {
    if (width <= 0) return '';
    const points = [...text];
    if (points.length <= width) return text;
    if (width === 1) return '…';
    return `${points.slice(0, width - 1).join('')}…`;
}

/** Three significant figures, without a trailing `.0`. */
function trim(value: number): string {
    const fixed = value >= 100 ? value.toFixed(0) : value >= 10 ? value.toFixed(1) : value.toFixed(2);
    return fixed.replace(/\.?0+$/, '');
}

function pad(value: number): string {
    return value < 10 ? `0${value}` : `${value}`;
}
