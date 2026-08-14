/** @jsxImportSource @sigx/runtime-core */
/**
 * The shell: tabs, status line, and the poll loop's lifetime.
 *
 * Five tabs, the same five the terminal has and in the same order, because
 * that ordering is design work already done. The host drill-down is a second
 * rendering of the Hosts tab rather than a sixth tab — `esc`/back returns to
 * the list, and the tab strip does not grow an entry that is only sometimes
 * meaningful.
 *
 * The status line is not decoration. **Stale data that still looks live is
 * the failure this dashboard would otherwise introduce**: a browser tab left
 * open overnight against a host that died at 03:00 renders a perfectly
 * plausible cluster. So the age of the last successful poll is always on
 * screen, and it turns `warn` once it exceeds three intervals.
 */
import { component, onMounted, onUnmounted, signal } from '@sigx/runtime-core';
import { DashboardState, type MonitorSource } from '@sigx/actors-monitor';
import { uptime } from '@sigx/actors-monitor/format';
import { ActorsPanel } from './panels/actors';
import { ClusterPanel } from './panels/cluster';
import { HealthPanel } from './panels/health';
import { HostPanel } from './panels/host';
import { HostsPanel } from './panels/hosts';
import { OverviewPanel } from './panels/overview';
import { panelState } from './panels/shared';
import { injectStyles } from './style';

export type TabId = 'overview' | 'hosts' | 'actors' | 'cluster' | 'health';

const TABS: readonly { id: TabId; label: string }[] = [
    { id: 'overview', label: 'Overview' },
    { id: 'hosts', label: 'Hosts' },
    { id: 'actors', label: 'Actors' },
    { id: 'cluster', label: 'Cluster' },
    { id: 'health', label: 'Health' }
];

export interface ActorsDashboardProps {
    /**
     * Where to read from — almost always
     * `httpSource({ url: '/your/ops/proxy' })`.
     *
     * **Do not pass an ops `secret` from browser code.** `ops()` reports your
     * actor type names, traffic shape and cluster topology, and actor keys
     * are user data; it also sets no CORS headers, so a browser cannot reach
     * it cross-origin anyway. Point this at a same-origin route of your own
     * app that authenticates the operator and attaches the bearer
     * server-side.
     */
    source: MonitorSource;
    /** Poll interval, ms. Default 1000. */
    intervalMs?: number;
    /** Sparkline history length, in samples. Default 60. */
    history?: number;
    /** Which tab opens first. Default `overview`. */
    tab?: TabId;
    /**
     * Inject the stylesheet. Default true.
     *
     * `false` if your CSP forbids injected styles or you extract CSS at build
     * time — `actorsDashboardCss` is exported for that.
     */
    styles?: boolean;
    /** Force a palette instead of following `prefers-color-scheme`. */
    theme?: 'light' | 'dark';
}

/**
 * The whole dashboard.
 *
 * Owns a `DashboardState` for its lifetime and stops it on unmount — which
 * matters more in a browser than in a CLI: a single-page app that navigates
 * away from an unstopped dashboard leaves it polling the cluster forever.
 */
export const ActorsDashboard = component<ActorsDashboardProps>((ctx) => {
    const view = signal<{ tab: TabId }>({ tab: ctx.props.tab ?? 'overview' });
    const state = new DashboardState({
        source: ctx.props.source,
        intervalMs: ctx.props.intervalMs,
        history: ctx.props.history
    });

    // Injected here rather than at module scope: this entry is imported in
    // bare Node by `scripts/verify-pack.js` and on a server during SSR, and
    // neither has a `document`.
    if (ctx.props.styles !== false) injectStyles();

    onMounted(() => state.start());
    onUnmounted(() => void state.stop());

    return () => (
        <div class="sxad" data-sxad-theme={ctx.props.theme}>
            <div class="sxad-bar">
                <div class="sxad-tabs" role="tablist">
                    {TABS.map((tab) => (
                        <button
                            type="button"
                            role="tab"
                            class="sxad-tab"
                            aria-selected={view.tab === tab.id ? 'true' : 'false'}
                            onClick={() => {
                                view.tab = tab.id;
                                // Leaving the Hosts tab closes the drill-down.
                                // It is not just a view: an open one makes the
                                // selected host walk its activation table on
                                // every poll, and nobody should pay for a
                                // panel they navigated away from.
                                if (tab.id !== 'hosts') state.focus(null);
                            }}
                        >
                            {tab.label}
                        </button>
                    ))}
                </div>
                <StatusLine state={state} />
            </div>
            <div class="sxad-body" role="tabpanel">
                {view.tab === 'hosts' ? (
                    <HostsTab state={state} />
                ) : view.tab === 'actors' ? (
                    <ActorsPanel state={state} />
                ) : view.tab === 'cluster' ? (
                    <ClusterPanel state={state} />
                ) : view.tab === 'health' ? (
                    <HealthPanel state={state} />
                ) : (
                    <OverviewPanel state={state} />
                )}
            </div>
        </div>
    );
});

/**
 * The Hosts tab: the list, or the selected host in full.
 *
 * Its own component rather than a branch in the shell, so that opening a
 * drill-down re-renders this subtree and not the tab strip above it — the
 * same reason each panel is a component at all.
 */
const HostsTab = component<{ state: DashboardState }>((ctx) => () => {
    const state = panelState(ctx.props);
    const focus = state.view.focus;
    return focus ? <HostPanel state={state} hostId={focus} /> : <HostsPanel state={state} />;
});

/**
 * What is being watched, how often, and how old the numbers are.
 *
 * `age` appears only once the data is stale by more than three intervals —
 * a permanently-visible "1s" trains you to ignore it, and the whole point is
 * that it catches your eye the once it matters.
 */
const StatusLine = component<{ state: DashboardState }>((ctx) => {
    const state = panelState(ctx.props);
    // Its own ticker, because staleness advances with the WALL CLOCK and not
    // with the poll count: when polling has stopped answering, nothing else
    // re-renders this line, and that is exactly when it has something to say.
    const clock = signal({ now: Date.now() });
    let timer: ReturnType<typeof setInterval> | null = null;
    onMounted(() => {
        timer = setInterval(() => {
            clock.now = Date.now();
        }, 1000);
        timer.unref?.();
    });
    onUnmounted(() => {
        if (timer) clearInterval(timer);
    });

    return () => {
        const view = state.view;
        const stale = view.lastOk === 0 ? null : clock.now - view.lastOk;
        return (
            <div class="sxad-status">
                <span>
                    src <b>{state.source.label}</b>
                </span>
                <span>
                    every <b>{`${view.intervalMs}ms`}</b>
                </span>
                {view.error ? (
                    <span class="sxad-danger">
                        poll <b>FAILING</b>
                    </span>
                ) : null}
                {stale !== null && stale > view.intervalMs * 3 ? (
                    <span class="sxad-warn">
                        age <b>{uptime(stale)}</b>
                    </span>
                ) : null}
                {view.snapshot?.partial ? (
                    <span class="sxad-warn">
                        totals <b>PARTIAL</b>
                    </span>
                ) : null}
            </div>
        );
    };
});
