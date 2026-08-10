/** @jsxImportSource @sigx/terminal */
/**
 * Does the dashboard render as LINES, and does it fit the pane it was given?
 *
 * Two failures live here, and neither is visible to a content assertion.
 *
 * **Structure.** `screens.test.tsx` used to join the rendered lines before
 * matching, which cannot tell twenty lines from one — so every screen
 * shipped with every row concatenated and the suite stayed green. The rule
 * underneath: `Text` is "deliberately INLINE, unlike every other
 * component", so consecutive spans share a line and rows have to be blocks.
 *
 * **Fit.** Until `@sigx/cli` 0.9 (signalxjs/cli#88) a tab could not learn
 * its pane, so nothing here knew the width: content clipped at the right
 * edge while most of the screen sat empty. Every screen now takes a pane,
 * and every screen is asserted against a wide one and a cramped one —
 * because "fits at 100×30" says nothing about an ssh window.
 */
import { describe, expect, it } from 'vitest';
import { displayWidth, render, renderNodeToLines } from '@sigx/terminal';
import {
    ClusterScreen,
    GrainsScreen,
    HealthScreen,
    OverviewScreen,
    HostsScreen,
    type Pane
} from '../src/dashboard/screens';
import { PANES, cursorModel, demoState, host } from './fixture';

/** Render to the line array the terminal actually receives. */
export function lines(node: unknown): string[] {
    const container = { type: 'element', tag: 'box', props: {}, children: [] } as never;
    render(node as never, container);
    return renderNodeToLines(container).map((line) => line.replace(/\[[0-9;]*m/g, ''));
}

/** Lines with visible content, trimmed — ignoring frame padding. */
export function content(node: unknown): string[] {
    return lines(node)
        .map((line) => line.trim())
        .filter((line) => line.length > 0);
}

/** Every screen, built for one pane. */
function screensAt(pane: Pane): [string, unknown][] {
    const state = demoState();
    return [
        ['Overview', <OverviewScreen state={state} pane={pane} />],
        ['Hosts', <HostsScreen state={state} pane={pane} cursor={cursorModel(0)} />],
        ['Actors', <GrainsScreen state={state} pane={pane} cursor={cursorModel(1)} />],
        ['Cluster', <ClusterScreen state={state} pane={pane} />],
        ['Health', <HealthScreen state={state} pane={pane} />]
    ];
}

describe.each(Object.entries(PANES))('every screen fits a %s pane', (_name, pane) => {
    it.each(screensAt(pane))('%s stays inside the width', (_screen, node) => {
        for (const line of lines(node)) {
            // Overflow is why content ran off the right edge of the
            // terminal. Measured in display CELLS: a wide glyph is two, so
            // `.length` would call an overflowing line a fitting one.
            expect(displayWidth(line)).toBeLessThanOrEqual(pane.width);
        }
    });

    it.each(screensAt(pane))('%s stays inside the height', (_screen, node) => {
        // A screen taller than its pane is not clipped harmlessly: the shell
        // draws the top and the bottom rows — the table's own footer, the
        // storage totals — silently vanish.
        expect(lines(node).length).toBeLessThanOrEqual(pane.height);
    });
});

describe('rows render as lines', () => {
    it('gives the overview totals one line per pair', () => {
        const out = content(<OverviewScreen state={demoState()} pane={PANES.wide} />);
        const hosts = out.findIndex((line) => line.startsWith('hosts'));
        expect(hosts).toBeGreaterThan(-1);
        // Concatenated, these read "hosts 2activations 32queued 2" — which
        // is what shipped.
        expect(out[hosts]).toContain('2');
        expect(out[hosts]).not.toContain('activations');
        expect(out[hosts + 1]).toContain('activations');
        expect(out[hosts + 2]).toContain('queued');
    });

    it('gives the host table a header line plus one line per host', () => {
        const state = demoState();
        state.view.snapshot = {
            ...state.view.snapshot!,
            hosts: [host(), host({ hostId: 's.b' }), host({ hostId: 's.c' })]
        };
        const out = content(<HostsScreen state={state} pane={PANES.wide} cursor={cursorModel(0)} />);
        const header = out.findIndex((line) => line.includes('HOST') && line.includes('STATUS'));
        expect(header).toBeGreaterThan(-1);
        // A row must not carry the next row's data, nor the header its rows.
        expect(out[header]).not.toContain('s.2sme5hx2');
        expect(out[header + 2]).toContain('s.2sme5hx2');
        expect(out[header + 3]).toContain('s.b');
        expect(out[header + 2]).not.toContain('s.b');
    });

    it('windows the table to the pane rather than drawing every row', () => {
        const state = demoState();
        const many = Array.from({ length: 200 }, (_, i) => host({ hostId: `s.${i}` }));
        state.view.snapshot = { ...state.view.snapshot!, hosts: many };
        const out = lines(<HostsScreen state={state} pane={PANES.narrow} cursor={cursorModel(0)} />);
        // 200 hosts in a 20-row pane: the viewport is what `height` buys,
        // and without it the frame would be ten screens long.
        expect(out.length).toBeLessThanOrEqual(PANES.narrow.height);
        expect(out.some((line) => line.includes('s.0'))).toBe(true);
        expect(out.some((line) => line.includes('s.199'))).toBe(false);
    });

    it('keeps a wide-glyph actor key inside the pane', () => {
        const state = demoState();
        state.view.snapshot = {
            ...state.view.snapshot!,
            activations: [
                // An actor key is user data. Measured by `.length` this looks
                // to FIT while overflowing the terminal — the exact bug
                // display-cell measurement exists for, so an assertion using
                // `.length` could never have caught it.
                { type: 'Counter', key: '用户名称超长的键-42', queued: 1, ageMs: 1000, idleMs: 0, keptAlive: false, tasks: 0, watchLoops: 0, watchSubscribers: 0 }
            ]
        };
        const out = lines(<GrainsScreen state={state} pane={PANES.narrow} cursor={cursorModel(0)} />);
        for (const line of out) expect(displayWidth(line)).toBeLessThanOrEqual(PANES.narrow.width);
        expect(out.join('\n')).toMatch(/…/);
    });

    it('scrolls the viewport to wherever the cursor is', () => {
        // The cursor has to DO something — a selection that never changes
        // what is on screen is the same as no selection, which is what the
        // Hosts table shipped with.
        //
        // Asserted through the viewport rather than through the `❯` marker:
        // `DataTable` draws that only while it holds focus, and in a shell
        // whose command input is focusable this table generally does not.
        // The row is coloured instead, and where the window lands is the
        // part that is observable as structure rather than as styling.
        const state = demoState();
        const many = Array.from({ length: 200 }, (_, i) => host({ hostId: `s.${i}` }));
        state.view.snapshot = { ...state.view.snapshot!, hosts: many };
        const top = content(<HostsScreen state={state} pane={PANES.wide} cursor={cursorModel(0)} />).join('\n');
        const deep = content(<HostsScreen state={state} pane={PANES.wide} cursor={cursorModel(150)} />).join('\n');
        expect(top).toContain('s.0 ');
        expect(top).not.toContain('s.150');
        expect(deep).toContain('s.150');
        expect(deep).not.toContain('s.0 ');
    });
});
