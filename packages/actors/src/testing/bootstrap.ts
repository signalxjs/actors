/**
 * The schema-bootstrap conformance suite: what "this provider's `ensure…Schema`
 * is safe to run from a booting host" means, as runnable cases.
 *
 * It exists because #76 was not a SurrealDB quirk. Two replicas booting at the
 * same time both run the bootstrap — that is a rolling deploy, the normal case
 * — and every store answers a concurrent `CREATE … IF NOT EXISTS` differently:
 * SurrealDB raises a write conflict whose wording changes between versions,
 * Postgres raises `23505`/`42P07`/`40P01` from the catalog. Both crashed a boot
 * for the same reason, and each package would otherwise have had to rediscover
 * it.
 *
 * **The governing rule: assert the OUTCOME, never the mechanism.** Postgres has
 * an advisory lock and uses it; SurrealDB verifiably has no lock primitive and
 * converges by jittered retry instead. A case that pinned "took a lock" or
 * "retried N times" would be untrue of one of them while telling us nothing
 * about the thing that actually matters — that every replica's boot survives.
 */
import type { ActorStorage } from '../types';
import type { ConformanceCase, ConformanceSkip } from './conformance';

// ---------------------------------------------------------------------------
// The harness a provider supplies

export interface BootstrapConformanceHarness {
    /**
     * Run this package's own bootstrap (`ensureSurrealSchema`, `ensurePgSchema`,
     * …) on a connection of its own, ALREADY ESTABLISHED, and with no retry
     * layer the harness added itself.
     *
     * All three are load-bearing, and the middle one is the trap:
     *
     *  - **Its own connection.** Sharing one serialises the writes inside the
     *    client and the concurrency case passes vacuously.
     *  - **Already connected.** `bootstrap()` must issue the DDL and nothing
     *    else — open the socket in the factory, ahead of the race. Measured on
     *    SurrealDB 3.2.4: eight racers that connect *then* bootstrap produce
     *    **0** conflicts, because connection setup staggers them past each
     *    other; eight pre-opened sockets firing the same DDL together produce
     *    **six**. A harness that connects inside `bootstrap()` therefore tests
     *    nothing and reports success.
     *  - **No harness-added retry.** Otherwise the case measures the driver's
     *    retry wrapper rather than the bootstrap. #76 arrived on a
     *    caller-owned connection carrying the driver's DEFAULT posture, and
     *    that is the configuration that has to converge.
     *
     * Optional, so a provider with no DDL at all (redis, k8s, cloudflare) gets
     * a visible SKIP rather than a false pass.
     */
    bootstrap?(): Promise<void>;
    /**
     * Storage on the bootstrapped schema — the proof it is USABLE, not merely
     * that the DDL resolved. A bootstrap can "succeed" against a schema whose
     * tables never appeared; only a round-trip catches that.
     */
    storage(): ActorStorage;
    /** Drop the namespace/schema and close every connection opened. */
    stop(): Promise<void>;
}

export type BootstrapConformanceFactory = () => Promise<BootstrapConformanceHarness>;

/**
 * Concurrent bootstraps per race. Two reproduces #76; eight makes it land
 * reliably enough that a regression fails the suite rather than the third
 * re-run of it.
 */
export const BOOTSTRAP_RACERS = 8;

// ---------------------------------------------------------------------------
// Assertions — deliberately framework-free

function assert(condition: unknown, message: string): asserts condition {
    if (!condition) throw new Error(`[bootstrap conformance] ${message}`);
}

/** Run `body` against a fresh harness and always tear it down. */
async function withHarness<T>(
    create: BootstrapConformanceFactory,
    body: (harness: BootstrapConformanceHarness) => Promise<T>
): Promise<T> {
    const harness = await create();
    try {
        return await body(harness);
    } finally {
        await harness.stop().catch(() => {});
    }
}

const NO_BOOTSTRAP: ConformanceSkip = {
    skipped: 'this provider has no schema bootstrap — there is nothing to race'
};

/** save → load → clear on one key, so "usable" means more than "defined". */
async function roundTrip(storage: ActorStorage, key: string): Promise<void> {
    const etag = await storage.save('BootstrapConformance', key, { n: 1 }, null);
    const loaded = await storage.load('BootstrapConformance', key);
    assert(loaded !== null, `the state saved under "${key}" did not load back`);
    assert(
        JSON.stringify(loaded.state) === JSON.stringify({ n: 1 }),
        `round-tripped state differs: ${JSON.stringify(loaded.state)}`
    );
    await storage.clear('BootstrapConformance', key, etag);
    assert(
        (await storage.load('BootstrapConformance', key)) === null,
        `the state under "${key}" survived clear()`
    );
}

// ---------------------------------------------------------------------------
// The cases

const bootstrapMakesStorageUsable: ConformanceCase<BootstrapConformanceFactory> = {
    name: 'one bootstrap leaves storage usable',
    why: 'a bootstrap that resolves without creating usable tables moves the failure from boot to every actor\'s first save',
    run: (create) =>
        withHarness(create, async (h) => {
            if (!h.bootstrap) return NO_BOOTSTRAP;
            await h.bootstrap();
            await roundTrip(h.storage(), 'once');
        })
};

const repeatedBootstrapConverges: ConformanceCase<BootstrapConformanceFactory> = {
    name: 'bootstrapping twice in a row is a no-op the second time',
    why: 'one statement missing IF NOT EXISTS makes every restart after the first a crash',
    run: (create) =>
        withHarness(create, async (h) => {
            if (!h.bootstrap) return NO_BOOTSTRAP;
            await h.bootstrap();
            await h.bootstrap();
            await roundTrip(h.storage(), 'twice');
        })
};

const concurrentBootstrapsConverge: ConformanceCase<BootstrapConformanceFactory> = {
    name: `${BOOTSTRAP_RACERS} concurrent bootstraps all converge`,
    why: 'two replicas booting at once IS a rolling deploy — one of them crashing on a DDL conflict is #76',
    run: (create) =>
        withHarness(create, async (h) => {
            if (!h.bootstrap) return NO_BOOTSTRAP;
            const bootstrap = h.bootstrap.bind(h);
            const results = await Promise.allSettled(
                Array.from({ length: BOOTSTRAP_RACERS }, () => bootstrap())
            );
            const failed = results.filter((r) => r.status === 'rejected');
            assert(
                failed.length === 0,
                `${failed.length}/${BOOTSTRAP_RACERS} bootstraps rejected — a booting replica ` +
                    `would have crashed. First: ${String((failed[0] as PromiseRejectedResult | undefined)?.reason)}`
            );
            // And the winner of the race left a schema that WORKS — a racer
            // that gave up early could otherwise resolve against nothing.
            await roundTrip(h.storage(), 'raced');
        })
};

/**
 * The suite. Cheapest and most fundamental first, so a broken harness fails on
 * "can it bootstrap at all" rather than inside the race.
 */
export const bootstrapConformance: readonly ConformanceCase<BootstrapConformanceFactory>[] = [
    bootstrapMakesStorageUsable,
    repeatedBootstrapConverges,
    concurrentBootstrapsConverge
];
