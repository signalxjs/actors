/**
 * Table layout — columns, alignment, and a scrolling viewport.
 *
 * `@sigx/terminal`'s `Table` is `columns: string[]` / `rows: string[][]`:
 * static, no scrolling, no sorting, no selection. A dashboard needs all
 * three, so the layout maths lives here (pure and testable) with a thin JSX
 * component over it. Intended to upstream once settled.
 */

/**
 * Display width in terminal CELLS, not code units.
 *
 * A wide glyph (CJK, most emoji) occupies two cells, so `.length` under-pads
 * and every column after it shifts left. Grain keys are user data, so this
 * is reachable with an ordinary key like `用户-42`: two wide glyphs (4
 * cells) plus `-42` (3) is 7, where `.length` says 5.
 *
 * Upstream is adding `fitCell`/`padCell` to `@sigx/terminal-zero`
 * (signalxjs/terminal#103); this goes away when they land.
 */
export function cellWidth(text: string): number {
    let width = 0;
    for (const char of text) {
        const code = char.codePointAt(0) ?? 0;
        // Zero-width: combining marks and joiners add no cells.
        if (code === 0x200d || (code >= 0x0300 && code <= 0x036f)) continue;
        width += isWide(code) ? 2 : 1;
    }
    return width;
}

/** The East Asian Wide / Fullwidth ranges, plus emoji. */
function isWide(code: number): boolean {
    return (
        (code >= 0x1100 && code <= 0x115f) || // Hangul Jamo
        (code >= 0x2e80 && code <= 0xa4cf) || // CJK radicals … Yi
        (code >= 0xac00 && code <= 0xd7a3) || // Hangul syllables
        (code >= 0xf900 && code <= 0xfaff) || // CJK compatibility ideographs
        (code >= 0xfe30 && code <= 0xfe6f) || // CJK compatibility forms
        (code >= 0xff00 && code <= 0xff60) || // Fullwidth forms
        (code >= 0xffe0 && code <= 0xffe6) ||
        (code >= 0x1f300 && code <= 0x1f64f) || // emoji
        (code >= 0x1f900 && code <= 0x1f9ff) ||
        (code >= 0x20000 && code <= 0x3fffd) // CJK extensions
    );
}

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
        let widest = cellWidth(column.header);
        for (const line of cells) widest = Math.max(widest, cellWidth(line[index]!));
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
    const actual = cellWidth(text);
    if (actual === width) return text;
    if (actual > width) {
        // Marked, not silent: a cut identity that looks complete is how you
        // end up chasing the wrong grain.
        if (width === 1) return '…';
        // Truncate by CELLS, so a wide glyph is never split in half and the
        // result never exceeds the column it was cut to fit.
        let out = '';
        let used = 0;
        for (const char of text) {
            const next = used + cellWidth(char);
            if (next > width - 1) break;
            out += char;
            used = next;
        }
        return `${out}${'…'}${' '.repeat(Math.max(0, width - used - 1))}`;
    }
    const pad = ' '.repeat(width - actual);
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
