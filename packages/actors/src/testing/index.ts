/**
 * `@sigx/actors/testing` — the shared conformance suites a provider package
 * runs against its own harness.
 *
 * Alias-only inside this workspace (no `package.json` exports entry), the same
 * standing policy as `@sigx/actors/cluster/testing`.
 *
 * The storage suite of #65 belongs here too, and composes rather than
 * duplicates: `BootstrapConformanceHarness.storage()`/`stop()` are the shared
 * intersection, and `bootstrap?()` is optional, so the harness a provider
 * writes for this suite is already the one that suite will want.
 */
export {
    bootstrapConformance,
    BOOTSTRAP_RACERS,
    type BootstrapConformanceFactory,
    type BootstrapConformanceHarness
} from './bootstrap';
export type { ConformanceCase, ConformanceSkip } from './conformance';
