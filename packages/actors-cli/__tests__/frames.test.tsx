/** @jsxImportSource @sigx/terminal */
/**
 * What every screen actually looks like, captured.
 *
 * The bug this rebuild fixes shipped because nobody ever looked at the
 * output: the assertions matched content, the content was all there, and
 * the screens rendered as one concatenated line for weeks. Content
 * assertions cannot catch that; neither can an assertion about line counts
 * catch a column that has drifted two cells left.
 *
 * So the whole frame is a snapshot. It is the cheapest possible way to make
 * a rendering regression arrive as a DIFF in a pull request rather than as
 * a screenshot months later — and reviewing the diff is reviewing the
 * dashboard.
 *
 * Non-TTY on purpose: no colour, no terminal size, no timing. The fixture
 * is fixed (`./fixture`), so a change here means the rendering changed.
 */
import { describe, expect, it } from 'vitest';
import { render, renderNodeToLines } from '@sigx/terminal';
import {
    ClusterScreen,
    GrainsScreen,
    HealthScreen,
    OverviewScreen,
    SilosScreen
} from '../src/dashboard/screens';
import { PANES, cursorModel, demoState } from './fixture';

/** The frame as the terminal receives it, minus the styling. */
function frame(node: unknown): string {
    const container = { type: 'element', tag: 'box', props: {}, children: [] } as never;
    render(node as never, container);
    return renderNodeToLines(container)
        .map((line) => line.replace(new RegExp(`${String.fromCharCode(27)}\\[[0-9;]*m`, 'g'), '').replace(/\s+$/, ''))
        .join('\n');
}

describe('rendered frames', () => {
    const state = demoState();

    it('Overview', () => {
        expect(frame(<OverviewScreen state={state} pane={PANES.wide} />)).toMatchSnapshot();
    });

    it('Silos', () => {
        expect(
            frame(<SilosScreen state={state} pane={PANES.wide} cursor={cursorModel(0)} />)
        ).toMatchSnapshot();
    });

    it('Grains', () => {
        expect(
            frame(<GrainsScreen state={state} pane={PANES.wide} cursor={cursorModel(1)} />)
        ).toMatchSnapshot();
    });

    it('Cluster', () => {
        expect(frame(<ClusterScreen state={state} pane={PANES.wide} />)).toMatchSnapshot();
    });

    it('Health', () => {
        expect(frame(<HealthScreen state={state} pane={PANES.wide} />)).toMatchSnapshot();
    });

    // The cramped pane is where layout decisions actually show: what wraps,
    // what drops, and what is still legible at an ssh window's width.
    it('Overview, cramped', () => {
        expect(frame(<OverviewScreen state={state} pane={PANES.narrow} />)).toMatchSnapshot();
    });

    it('Cluster, cramped', () => {
        expect(frame(<ClusterScreen state={state} pane={PANES.narrow} />)).toMatchSnapshot();
    });
});
