/** @jsxImportSource @sigx/runtime-core */
/**
 * The shell — tabs, the drill-down, and the status line.
 *
 * The status line carries most of these tests, because it exists to prevent
 * the one failure a browser dashboard introduces that a CLI does not: a tab
 * left open overnight against a host that died at 03:00 renders a perfectly
 * plausible cluster, indefinitely, with nothing on screen saying the numbers
 * stopped moving.
 */
import { afterEach, beforeEach, describe, expect, it, vi } from 'vitest';
import { ActorsDashboard } from '@sigx/actors-dashboard';
import type { MonitorSnapshot, MonitorSource, SnapshotOptions } from '@sigx/actors-monitor';
import { demoSnapshot } from '../../actors-monitor/__tests__/fixture';
import { mount } from './render';

/** A source under the test's control, recording what each poll asked for. */
function stubSource(): MonitorSource & {
    asks: SnapshotOptions[];
    answer: MonitorSnapshot | Error;
    closed: boolean;
} {
    const source = {
        kind: 'http' as const,
        label: 'http://127.0.0.1:5391',
        asks: [] as SnapshotOptions[],
        answer: { ...demoSnapshot } as MonitorSnapshot | Error,
        closed: false,
        snapshot(_signal?: AbortSignal, options?: SnapshotOptions): Promise<MonitorSnapshot> {
            source.asks.push(options ?? {});
            return source.answer instanceof Error
                ? Promise.reject(source.answer)
                : Promise.resolve(source.answer);
        },
        close(): Promise<void> {
            source.closed = true;
            return Promise.resolve();
        }
    };
    return source;
}

/** Let the poll loop's promise chain settle. */
const settle = () => new Promise<void>((resolve) => setTimeout(resolve, 0));

beforeEach(() => {
    vi.useFakeTimers({ shouldAdvanceTime: true });
});

afterEach(() => {
    vi.useRealTimers();
});

describe('ActorsDashboard', () => {
    it('starts polling on mount and stops on unmount', async () => {
        // A single-page app that navigates away from an unstopped dashboard
        // leaves it polling the cluster for the lifetime of the tab.
        const source = stubSource();
        const view = mount(<ActorsDashboard source={source} />);
        await settle();
        expect(source.asks.length).toBeGreaterThan(0);

        view.unmount();
        await settle();
        expect(source.closed).toBe(true);

        const after = source.asks.length;
        await vi.advanceTimersByTimeAsync(5000);
        expect(source.asks.length).toBe(after);
    });

    it('opens on Overview and switches tabs', async () => {
        const source = stubSource();
        const view = mount(<ActorsDashboard source={source} />);
        await settle();
        expect(view.text()).toContain('cluster · 2 host(s)');

        const tab = (label: string) =>
            view.all('.sxad-tab').find((button) => button.textContent === label) as HTMLElement;
        tab('Cluster').click();
        expect(view.text()).toContain('reminder shards');

        tab('Health').click();
        expect(view.text()).toContain('etag conflicts');
        view.unmount();
    });

    it('honours the initial tab', async () => {
        const source = stubSource();
        const view = mount(<ActorsDashboard source={source} tab="cluster" />);
        await settle();
        expect(view.text()).toContain('reminder shards');
        view.unmount();
    });

    it('asks for DETAIL only while a drill-down is open', async () => {
        // The expensive direction has to be the one you opt into: a detail
        // poll makes the selected host walk its activation table, and nobody
        // should pay for a panel they are not looking at.
        const source = stubSource();
        const view = mount(<ActorsDashboard source={source} tab="hosts" />);
        await settle();
        expect(source.asks.every((ask) => !ask.detail)).toBe(true);

        (view.all('tbody tr')[0] as HTMLElement).click();
        await settle();
        expect(source.asks.at(-1)).toEqual({ detail: true, hostId: 's.2sme5hx2' });

        // Leaving the tab closes it, so the fleet stops paying immediately.
        const tab = view.all('.sxad-tab').find((b) => b.textContent === 'Overview') as HTMLElement;
        tab.click();
        await vi.advanceTimersByTimeAsync(1100);
        expect(source.asks.at(-1)?.detail).toBeFalsy();
        view.unmount();
    });

    it('says the poll is FAILING while keeping the last good numbers', async () => {
        const source = stubSource();
        const view = mount(<ActorsDashboard source={source} />);
        await settle();
        expect(view.text()).toContain('activations');

        source.answer = new Error('connect ECONNREFUSED');
        await vi.advanceTimersByTimeAsync(1100);
        await settle();

        expect(view.text()).toContain('FAILING');
        expect(view.text()).toContain('poll failed');
        // The fleet's numbers are still there — blanking them destroys the
        // context you need to understand the outage.
        expect(view.text()).toContain('cluster · 2 host(s)');
        view.unmount();
    });

    it('shows the AGE of the data once it is stale, and not before', async () => {
        // The failure this whole line exists for: numbers that stopped moving
        // but still look live. `at` is stamped now rather than taken from the
        // fixture, whose fixed 2023 timestamp is already stale by a thousand
        // days — a correct reading, but not the one under test here.
        const source = stubSource();
        source.answer = { ...demoSnapshot, at: Date.now() };
        const view = mount(<ActorsDashboard source={source} intervalMs={1000} />);
        await settle();
        // Fresh. A permanently-visible "1s" trains you to ignore the line.
        expect(view.text()).not.toContain('age');

        // The source stops answering. Nothing else re-renders the status
        // line from here on, which is exactly when it has something to say —
        // hence its own wall-clock ticker.
        source.answer = new Error('gone');
        await vi.advanceTimersByTimeAsync(4000);
        expect(view.text()).toContain('age');
        view.unmount();
    });

    it('flags PARTIAL totals in the status line, not only in the banner', async () => {
        const source = stubSource();
        source.answer = { ...demoSnapshot, partial: true };
        const view = mount(<ActorsDashboard source={source} />);
        await settle();
        expect(view.text()).toContain('PARTIAL');
        view.unmount();
    });

    it('names what it is watching, so it is never ambiguous', async () => {
        const source = stubSource();
        const view = mount(<ActorsDashboard source={source} />);
        await settle();
        expect(view.text()).toContain('http://127.0.0.1:5391');
        view.unmount();
    });
});

describe('styles', () => {
    it('injects the stylesheet once, however many dashboards mount', async () => {
        const source = stubSource();
        const a = mount(<ActorsDashboard source={source} />);
        const b = mount(<ActorsDashboard source={stubSource()} />);
        await settle();
        expect(document.querySelectorAll('style[data-sigx-actors-dashboard]')).toHaveLength(1);
        a.unmount();
        b.unmount();
    });

    it('honours an explicit theme instead of prefers-color-scheme', async () => {
        const source = stubSource();
        const view = mount(<ActorsDashboard source={source} theme="dark" />);
        await settle();
        expect(view.one('.sxad')?.getAttribute('data-sxad-theme')).toBe('dark');
        view.unmount();
    });
});
