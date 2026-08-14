/**
 * The terminal half of the shared fixture.
 *
 * The dashboard STATE — two hosts with one draining, a hot actor, an
 * unclaimed reminder shard, a method that does not exist being called one
 * time in seven — is `@sigx/actors-monitor`'s, and is re-exported here rather
 * than copied. Both renderers assert against the same bytes on purpose: the
 * claim the layering makes is that a terminal and a browser over one data
 * layer cannot disagree about what the cluster is doing, and two fixtures
 * could not detect it if they did.
 *
 * What is added here is the two things only a terminal has: a `DataTable`
 * cursor model, and pane sizes.
 */
import { createModel, signal, type Model } from '@sigx/terminal';
import type { Pane } from '../src/dashboard/screens';

export {
    demoSnapshot,
    demoState,
    host,
    inertSource
} from '../../actors-monitor/__tests__/fixture';

/** A `DataTable` cursor, the way `top.ts` builds one. */
export function cursorModel(index = 0): Model<number> {
    const cursor = signal({ index });
    return createModel<number>([cursor, 'index'], (value) => {
        cursor.index = value;
    });
}

/** The pane sizes every screen is asserted against. */
export const PANES: Record<'wide' | 'narrow', Pane> = {
    wide: { width: 100, height: 30 },
    narrow: { width: 60, height: 20 }
};
