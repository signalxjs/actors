/** @jsxImportSource @sigx/terminal */
/**
 * Prints each screen so a human can LOOK at it.
 *
 * Not an assertion — a development aid, skipped by default. `frames.test.tsx`
 * is the one that fails when the rendering changes; this is for when you
 * want to see the thing at a size the snapshot does not cover, or with
 * colour left on.
 *
 *     npx vitest run packages/actors-cli/__tests__/eyeball.test.tsx -t print
 */
import { describe, it } from 'vitest';
import { render, renderNodeToLines } from '@sigx/terminal';
import {
    ClusterScreen,
    GrainsScreen,
    HealthScreen,
    OverviewScreen,
    SilosScreen,
    type Pane
} from '../src/dashboard/screens';
import { cursorModel, demoState } from './fixture';

function draw(node: unknown): string[] {
    const container = { type: 'element', tag: 'box', props: {}, children: [] } as never;
    render(node as never, container);
    return renderNodeToLines(container);
}

describe('eyeball', () => {
    it.skip('print', () => {
        const state = demoState();
        // Roughly what the shell leaves a tab in an 80×30 terminal.
        const pane: Pane = { width: 78, height: 26 };
        const screens: [string, unknown][] = [
            ['OVERVIEW', <OverviewScreen state={state} pane={pane} />],
            ['SILOS', <SilosScreen state={state} pane={pane} cursor={cursorModel(0)} />],
            ['GRAINS', <GrainsScreen state={state} pane={pane} cursor={cursorModel(1)} />],
            ['CLUSTER', <ClusterScreen state={state} pane={pane} />],
            ['HEALTH', <HealthScreen state={state} pane={pane} />]
        ];
        for (const [name, node] of screens) {
            const out = draw(node);
            console.log(`\n${'═'.repeat(70)}\n  ${name}  (${out.length} lines)\n${'═'.repeat(70)}`);
            for (const line of out) console.log(line);
        }
    });
});
