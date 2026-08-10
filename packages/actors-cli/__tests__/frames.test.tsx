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
    HostScreen,
    HostsScreen
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

    it('Hosts', () => {
        expect(
            frame(<HostsScreen state={state} pane={PANES.wide} cursor={cursorModel(0)} />)
        ).toMatchSnapshot();
    });

    it('Actors', () => {
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

    it('Host drill-down', () => {
        // What selecting a row in the Hosts table now opens. Before #121 it
        // could not exist: a `HostReport` carried no metrics, no health and
        // no actors for a peer, so the cursor moved and nothing changed.
        const focused = demoState();
        focused.view.focus = 's.2sme5hx2';
        focused.view.snapshot = {
            ...focused.view.snapshot!,
            hosts: focused.view.snapshot!.hosts.map((host, i) =>
                i === 0
                    ? {
                          ...host,
                          activations: [
                              { type: 'Counter', key: 'cart', queued: 3, ageMs: 167_000, idleMs: 2000, keptAlive: true, tasks: 1, watchLoops: 0, watchSubscribers: 0 },
                              { type: 'Counter', key: 'cold-0', queued: 0, ageMs: 161_000, idleMs: 2000, keptAlive: false, tasks: 0, watchLoops: 0, watchSubscribers: 0 }
                          ]
                      }
                    : host
            )
        };
        expect(
            frame(<HostScreen state={focused} pane={PANES.wide} hostId="s.2sme5hx2" />)
        ).toMatchSnapshot();
    });
});
