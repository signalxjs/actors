/**
 * Table layout — columns, alignment, and a scrolling viewport.
 *
 * `@sigx/terminal`'s `Table` is `columns: string[]` / `rows: string[][]`:
 * static, no scrolling, no sorting, no selection. A dashboard needs all
 * three, so the layout maths lives here (pure and testable) with a thin JSX
 * component over it. Intended to upstream once settled.
 */

export interface Column<T> {
    key: string;
    header: string;
    /** Cell text. Formatting belongs to the caller, not to the table. */
    value: (row: T) => string;
    /** Fixed width; otherwise sized to the widest cell. */
    width?: number;
    align?: 'left' | 'right';
    /**
     * Minimum width when the table has to shrink. Columns give up space in
     * reverse order so the leftmost — usually the identity — survives.
     */
    min?: number;
}

export interface LayoutOptions {
    /** Total columns available. Omit for natural width. */
    width?: number;
    /** Gap between columns. Default 2. */
    gap?: number;
}

/** One laid-out row: the cells, padded and aligned. */
export interface LaidOutTable {
    header: string;
    rows: string[];
    /** Final width of each column, in declaration order. */
    widths: number[];
}

/**
 * Lay out a table, fitting `width` if given.
 *
 * Shrinking takes space from the RIGHTMOST columns first. The left column
 * is nearly always the identity (a silo id, a grain key), and a table whose
 * identities are truncated to make room for a latency figure is unusable.
 *
 * Every returned line has its trailing padding trimmed — see `join` below.
 */
export function layoutTable<T>(
    columns: readonly Column<T>[],
    rows: readonly T[],
    options: LayoutOptions = {}
): LaidOutTable {
    const gap = options.gap ?? 2;
    const cells = rows.map((row) => columns.map((column) => column.value(row)));

    const widths = columns.map((column, index) => {
        if (column.width !== undefined) return Math.max(0, Math.floor(column.width));
        let widest = column.header.length;
        for (const line of cells) widest = Math.max(widest, line[index]!.length);
        return widest;
    });

    if (options.width !== undefined && Number.isFinite(options.width)) {
        shrinkToFit(widths, columns, Math.max(0, Math.floor(options.width)), gap);
    }

    const join = (values: string[]): string =>
        values
            .map((value, index) => fit(value, widths[index]!, columns[index]!.align ?? 'left'))
            .join(' '.repeat(gap))
            // Trailing padding is TRIMMED. It is invisible on its own, but
            // a selected row is painted to its own length, so keeping it
            // would extend the highlight past the last character by however
            // much the final column happened to be padded.
            .replace(/\s+$/, '');

    return {
        header: join(columns.map((column) => column.header)),
        rows: cells.map(join),
        widths
    };
}

/** Take space from the right until the table fits. */
function shrinkToFit<T>(
    widths: number[],
    columns: readonly Column<T>[],
    available: number,
    gap: number
): void {
    const gaps = Math.max(0, widths.length - 1) * gap;
    let total = widths.reduce((sum, width) => sum + width, 0) + gaps;
    for (let index = widths.length - 1; index >= 0 && total > available; index--) {
        const min = columns[index]!.min ?? Math.min(widths[index]!, 3);
        const give = Math.min(widths[index]! - min, total - available);
        if (give > 0) {
            widths[index]! -= give;
            total -= give;
        }
    }
}

/** Pad or truncate one cell. Truncation is always marked. */
export function fit(text: string, width: number, align: 'left' | 'right' = 'left'): string {
    if (width <= 0) return '';
    if (text.length === width) return text;
    if (text.length > width) {
        // Marked, not silent: a cut identity that looks complete is how you
        // end up chasing the wrong grain.
        return width === 1 ? '…' : `${text.slice(0, width - 1)}…`;
    }
    const pad = ' '.repeat(width - text.length);
    return align === 'right' ? pad + text : text + pad;
}

/**
 * Which slice of a list a viewport should show.
 *
 * Keeps the cursor in view while moving the window as little as possible —
 * so paging down one row scrolls by one row, not by a screenful. Returns
 * the new offset.
 */
export function scrollOffset(
    total: number,
    cursor: number,
    height: number,
    offset: number
): number {
    if (height <= 0 || total <= height) return 0;
    const clampedCursor = Math.max(0, Math.min(total - 1, cursor));
    let next = Math.max(0, Math.min(offset, total - height));
    if (clampedCursor < next) next = clampedCursor;
    else if (clampedCursor >= next + height) next = clampedCursor - height + 1;
    return next;
}

/** Move a cursor by `delta`, clamped rather than wrapped. */
export function moveCursor(cursor: number, delta: number, total: number): number {
    if (total <= 0) return 0;
    // Clamped, not wrapped: holding ↓ on a long list should stop at the
    // bottom, not silently return to the top and look like nothing happened.
    return Math.max(0, Math.min(total - 1, cursor + delta));
}

/**
 * Sort rows by a comparator, with a STABLE tiebreak on the row's identity.
 *
 * The tiebreak is the point. A dashboard re-sorts every poll, and rows with
 * equal values would otherwise swap places each time — motion that reads as
 * activity when nothing has changed.
 */
export function sortRows<T>(
    rows: readonly T[],
    compare: (a: T, b: T) => number,
    identity: (row: T) => string
): T[] {
    return [...rows].sort((a, b) => {
        const primary = compare(a, b);
        if (primary !== 0) return primary;
        const left = identity(a);
        const right = identity(b);
        return left < right ? -1 : left > right ? 1 : 0;
    });
}
