/**
 * `@sigx/actors-cli/source` — the transport-agnostic data layer.
 *
 * Exported as its own subpath because it is deliberately renderer-free: a
 * web dashboard is a likely follow-up and should consume this rather than
 * reimplement the awkward parts (rate derivation across resets, `partial`
 * fan-outs, reconciling the embedded and HTTP shapes).
 */
export {
    siloViewFromReport,
    type ClusterView,
    type MonitorSnapshot,
    type MonitorSource,
    type SiloView
} from './types';
export { httpSource, OpsRequestError, type HttpSourceOptions } from './http';
export { embeddedSource, EmbeddedSourceError, type EmbeddedSourceOptions } from './embedded';
export { rateBetween, RateTracker, Series, type Rate, type RateSample } from '../model/rates';
export * as format from '../model/format';
