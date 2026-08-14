/**
 * `@sigx/actors-cli/source` — the transport-agnostic data layer.
 *
 * It LIVES in `@sigx/actors-monitor` now (#239). That package is
 * browser-safe, has no renderer in it and no `@sigx/cli` peer, which is what
 * a web dashboard actually needs — this one hard-depends on `@sigx/terminal`
 * and peers a CLI, so importing it to reach `httpSource` cost a web app both.
 *
 * What is left here is the half that could never move: `embeddedSource`
 * dynamic-`import()`s the project's own app module and STARTS A REAL HOST.
 * Keeping it behind this subpath while the shared layer is a separate package
 * is a stronger split than any subpath could be — a bundler following
 * `@sigx/actors-monitor` cannot reach it at all.
 *
 * The re-exports below are kept so existing importers of this subpath are
 * unbroken. New consumers should import `@sigx/actors-monitor` directly.
 */
export {
    hostViewFromReport,
    type ClusterView,
    type MonitorSnapshot,
    type MonitorSource,
    type HostView
} from '@sigx/actors-monitor';
export { httpSource, OpsRequestError, type HttpSourceOptions } from '@sigx/actors-monitor';
export { rateBetween, RateTracker, Series, type Rate, type RateSample } from '@sigx/actors-monitor';
export { embeddedSource, EmbeddedSourceError, type EmbeddedSourceOptions } from './embedded';
export * as format from '@sigx/actors-monitor/format';
