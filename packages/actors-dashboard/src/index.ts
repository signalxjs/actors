/**
 * `@sigx/actors-dashboard` — the web rendering of `@sigx/actors-monitor`.
 *
 * The five tabs `sigx actors top` has, in a browser: Overview, Hosts (with a
 * per-host drill-down), Actors, Cluster, Health. Pure sigx — no CSS
 * framework, no chart library, and no `sigx` umbrella import (its first line
 * is `import '@sigx/runtime-dom/platform'`, which would drag a renderer in
 * behind every import of this package).
 *
 * ```tsx
 * import { ActorsDashboard } from '@sigx/actors-dashboard';
 * import { httpSource } from '@sigx/actors-monitor';
 *
 * <ActorsDashboard source={httpSource({ url: '/admin/ops' })} />
 * ```
 *
 * **`/admin/ops` there is a route of YOUR app, not the host's.** `ops()` sets
 * no CORS headers and refuses to construct without a bearer secret outside
 * dev — it reports your actor type names, traffic shape and cluster
 * topology, and actor keys are user data. So the browser calls a same-origin
 * route you own, which authenticates the operator however your app already
 * does and forwards to `ops()` with the bearer attached server-side — the
 * secret never reaches the client. `docs/architecture/monitoring.md` has the
 * whole nine-line proxy.
 *
 * Every panel is exported on its own and takes `{ state }`, so a portal that
 * wants one table rather than the whole shell can have it:
 *
 * ```tsx
 * const state = new DashboardState({ source });
 * state.start();
 * <HostsPanel state={state} />
 * ```
 *
 * Nothing here decides what a number MEANS. What is wrong, what a figure is
 * about, whether a counter moved backwards, whether a shard state is an
 * incident — all of it is `@sigx/actors-monitor`'s, so this rendering and the
 * terminal one cannot disagree. See `docs/architecture/monitoring.md`.
 */
export { ActorsDashboard, type ActorsDashboardProps, type TabId } from './dashboard';
export { mountActorsDashboard } from './mount';
export { OverviewPanel } from './panels/overview';
export { HostsPanel } from './panels/hosts';
export { HostPanel } from './panels/host';
export { ActorsPanel } from './panels/actors';
export { ClusterPanel } from './panels/cluster';
export { HealthPanel } from './panels/health';
export { panelState, type PanelProps } from './panels/shared';
export { actorsDashboardCss, injectStyles } from './style';
// The parts, for a portal building a panel of its own against the same
// vocabulary — a `Sparkline` that breaks on a gap is the one worth reusing.
export {
    Alerts,
    Bars,
    DataTable,
    DetailList,
    Section,
    ShardGrid,
    Series,
    Sparkline,
    type Column,
    type DetailRow,
    type Tone
} from './parts/primitives';
// Re-exported for convenience, so a consumer needs one import to get going.
// The data layer itself stays `@sigx/actors-monitor`'s — this is sugar, not
// ownership.
export { DashboardState, httpSource, type MonitorSource } from '@sigx/actors-monitor';
