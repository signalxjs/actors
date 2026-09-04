/**
 * `@sigx/actors/testing` — the shared conformance suites a provider package
 * runs against its own harness.
 *
 * Alias-only inside this workspace (no `package.json` exports entry), the same
 * standing policy as `@sigx/actors/cluster/testing`.
 *
 * The reminders suite (#385) drives every `ActorReminders` provider through
 * one list of outcomes — the five providers had each re-pinned the seam.
 *
 * The storage suite (#65) composes with the bootstrap suite rather than
 * duplicating it: `storage()`/`stop()` are the shared intersection and
 * `bootstrap?()` is optional in both, so the harness a provider writes for
 * one is already the one the other wants.
 */
export {
    bootstrapConformance,
    BOOTSTRAP_RACERS,
    type BootstrapConformanceFactory,
    type BootstrapConformanceHarness
} from './bootstrap';
export {
    storageConformance,
    type StorageConformanceFactory,
    type StorageConformanceHarness
} from './storage';
export {
    socketTransportConformance,
    type SocketTransportFactory,
    type SocketTransportHarness
} from './socket-transport';
export {
    remindersConformance,
    type RemindersConformanceFactory,
    type RemindersConformanceHarness
} from './reminders';
export type { ConformanceCase, ConformanceSkip } from './conformance';
