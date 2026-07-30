/** @jsxImportSource @sigx/terminal */
/**
 * Does the dashboard render as LINES?
 *
 * `screens.test.tsx` asserts on content and joins the rendered lines before
 * matching, which cannot tell twenty lines from one — so every screen
 * shipped with every row concatenated onto a single line and the suite
 * stayed green. These assertions are on the line ARRAY: how many, how wide,
 * and which text lands on which.
 *
 * The underlying rule (terminal-zero's own words): `<text>` is
 * "deliberately INLINE, unlike every other component". Consecutive `<text>`
 * siblings share a line. Rows have to be block elements.
 */
import { describe, expect, it } from 'vitest';
import { render, renderNodeToLines } from '@sigx/terminal';
import { KeyValue, DataTable } from '../src/tui/components';
import { cellWidth, type Column } from '../src/tui/table';

/** Render to the line array the terminal actually receives. */
export function lines(node: unknown): string[] {
    const container = { type: 'element', tag: 'box', props: {}, children: [] } as never;
    render(node as never, container);
    return renderNodeToLines(container);
}

/** Lines with visible content, trimmed — ignoring frame padding. */
export function content(node: unknown): string[] {
    return lines(node)
        .map((line) => line.replace(/\[[0-9;]*m/g, '').trim())
        .filter((line) => line.length > 0);
}

interface Row {
    id: string;
    n: number;
}

const columns: Column<Row>[] = [
    { key: 'id', header: 'ID', value: (r) => r.id },
    { key: 'n', header: 'N', value: (r) => String(r.n), align: 'right' }
];

describe('rows render as lines', () => {
    it('gives KeyValue one line per pair', () => {
        const out = content(
            <KeyValue
                rows={[
                    { label: 'silos', value: '2' },
                    { label: 'activations', value: '32' },
                    { label: 'queued', value: '0' }
                ]}
            />
        );
        // Three pairs, three lines. Concatenated, this is one line reading
        // "silos 2activations 32queued 0" — which is what shipped.
        expect(out).toHaveLength(3);
        expect(out[0]).toContain('silos');
        expect(out[0]).toContain('2');
        expect(out[1]).toContain('activations');
        expect(out[2]).toContain('queued');
        // The giveaway: a value must never share its line with the NEXT label.
        expect(out[0]).not.toContain('activations');
    });

    it('gives DataTable a header line plus one line per row', () => {
        const out = content(
            <DataTable
                columns={columns}
                rows={[
                    { id: 'silo-a', n: 30 },
                    { id: 'silo-b', n: 2 }
                ]}
                cursor={0}
            />
        );
        expect(out).toHaveLength(3);
        expect(out[0]).toContain('ID');
        expect(out[1]).toContain('silo-a');
        expect(out[2]).toContain('silo-b');
        // A row must not carry the next row's data, nor the header its rows.
        expect(out[0]).not.toContain('silo-a');
        expect(out[1]).not.toContain('silo-b');
    });

    it('marks exactly one row, on its own line', () => {
        const out = content(
            <DataTable
                columns={columns}
                rows={[
                    { id: 'a', n: 1 },
                    { id: 'b', n: 2 }
                ]}
                cursor={1}
            />
        );
        const marked = out.filter((line) => line.includes('▸'));
        expect(marked).toHaveLength(1);
        expect(marked[0]).toContain('b');
    });

    it('never emits a line wider than the width it was given', () => {
        const width = 30;
        const out = lines(
            <DataTable
                columns={columns}
                rows={[
                    { id: 'a-very-long-silo-identifier-here', n: 123456 },
                    // A wide-glyph key. Measured with `.length` this looks
                    // to FIT while overflowing the terminal — the exact bug
                    // `cellWidth` exists for, so an assertion using `.length`
                    // could never have caught it.
                    { id: '用户名称超长的键', n: 1 }
                ]}
                width={width}
            />
        ).map((line) => line.replace(/\[[0-9;]*m/g, ''));
        // Overflow is why content ran off the right edge of the terminal.
        for (const line of out) expect(cellWidth(line)).toBeLessThanOrEqual(width + 2);
    });
});
