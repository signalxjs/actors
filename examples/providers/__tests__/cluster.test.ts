/**
 * @vitest-environment node
 *
 * The shared walk (`src/cluster.ts`) on the seams no infrastructure is
 * needed for — `memoryStorage()` + `memoryClusterHub()` on ephemeral
 * ports — so the harness every provider demo stands on is itself proven,
 * and a step that asserts a wrong thing fails HERE rather than only when
 * someone has a Postgres to hand.
 */
import { afterEach, describe, expect, it } from 'vitest';
import { memoryStorage } from '@sigx/actors/host';
import { memoryClusterHub } from '@sigx/actors/cluster';
import { prometheusOps } from '@sigx/actors-otel/prometheus';
import { Counter } from '../src/counter.actor.ts';
import {
    CROSS_HOST_COUNT,
    envOr,
    parsePorts,
    startCluster,
    type DemoCluster,
    type DemoClusterOptions
} from '../src/cluster.ts';
import { parseExposition, sampleValue } from '../src/prometheus.ts';

const SECRET = 'test-ops-secret';
const running: DemoCluster[] = [];
const lines: string[] = [];

async function start(extra: Partial<DemoClusterOptions> = {}): Promise<DemoCluster> {
    const hub = memoryClusterHub();
    const demo = await startCluster({
        label: 'memoryStorage()',
        storage: memoryStorage(),
        providers: () => hub.providers(),
        // Ephemeral ports: the harness reads the bound port back and
        // advertises THAT, so three suites can run at once.
        ports: [0, 0, 0],
        opsSecret: SECRET,
        log: (...args) => lines.push(args.map(String).join(' ')),
        ...extra
    });
    running.push(demo);
    return demo;
}

afterEach(async () => {
    lines.length = 0;
    for (const demo of running.splice(0)) await demo.stop();
});

describe('the shared walk', () => {
    it('spreads, keeps one activation, crosses hosts, fails over and reports', async () => {
        const demo = await start();
        expect(demo.members).toHaveLength(3);
        expect(new Set(demo.members.map((m) => m.port)).size).toBe(3);

        const spread = await demo.spread();
        expect(Object.values(spread).reduce((a, b) => a + b, 0)).toBe(9);

        const { owner, results } = await demo.singleActivation();
        expect(results).toEqual([1, 2, 3, 4, 5, 6]);

        const { via, count } = await demo.crossHost(owner);
        expect(via).not.toBe(owner);
        expect(count).toBe(CROSS_HOST_COUNT);

        const { survivor, count: recovered, reclaimedBy } = await demo.failover(owner);
        expect(survivor).not.toBe(owner);
        expect(recovered).toBe(CROSS_HOST_COUNT);
        expect(demo.members.map(demo.hostId)).toContain(reclaimedBy);
        expect(reclaimedBy).not.toBe(demo.hostId(owner));
        expect(owner.server.listening).toBe(false);

        const report = await demo.report(survivor);
        expect(report.view.size).toBe(2);
        expect(report.hosts.map((h) => h.hostId).sort()).toEqual(
            demo.members
                .filter((m) => m !== owner)
                .map(demo.hostId)
                .sort()
        );
        expect(report.partial).toBe(false);
    });

    /**
     * The tcp demo adds a burst of its own between step 3 and failover.
     * Failover must read back what the actor actually holds — `expected`
     * says what that is — and a demo that forgets to say so fails loudly
     * rather than passing on a stale constant.
     */
    it('failover asserts the count the demo says to expect', async () => {
        const demo = await start();
        await demo.spread();
        const { owner } = await demo.singleActivation();
        await demo.crossHost(owner);
        const via = demo.members.find((m) => m !== owner)!;
        await Promise.all(
            Array.from({ length: 5 }, () => via.host.actor(Counter, demo.key('cart')).increment(1))
        );
        const { count } = await demo.failover(owner, CROSS_HOST_COUNT + 5);
        expect(count).toBe(CROSS_HOST_COUNT + 5);
    });

    it('failover with the default expectation rejects after extra calls', async () => {
        const demo = await start();
        await demo.spread();
        const { owner } = await demo.singleActivation();
        await demo.crossHost(owner);
        const via = demo.members.find((m) => m !== owner)!;
        await via.host.actor(Counter, demo.key('cart')).increment(1);
        await expect(demo.failover(owner)).rejects.toThrow(/state lost in failover/);
    });

    it('numbers the steps in the order they ran, a demo step included', async () => {
        const demo = await start();
        await demo.spread();
        const { owner } = await demo.singleActivation();
        await demo.crossHost(owner);
        demo.step('Something of the demo’s own');
        await demo.failover(owner);
        const headers = lines.filter((l) => l.startsWith('\n=== ')).map((l) => l.slice(5, 7));
        expect(headers).toEqual(['1.', '2.', '3.', '4.', '5.']);
        expect(lines.some((l) => l.includes('4. Something of the demo'))).toBe(true);
    });

    it('keys are per run, so a persistent store starts fresh', async () => {
        const demo = await start();
        expect(demo.key('cart')).toBe(`cart-${demo.run}`);
        expect(demo.run).toMatch(/^[0-9a-z]+$/);
    });
});

describe('the exposition reader against the real exporter', () => {
    it('parses what prometheusOps() serves, behind the same bearer as ops()', async () => {
        const demo = await start({ plugins: () => [prometheusOps({ secret: SECRET })] });
        await demo.spread();
        const [first] = demo.members;
        const url = `http://127.0.0.1:${first!.port}/_sigx/metrics`;

        const denied = await fetch(url);
        expect(denied.status).toBe(401);

        const scrape = await fetch(url, { headers: { authorization: `Bearer ${SECRET}` } });
        expect(scrape.status).toBe(200);
        const samples = parseExposition(await scrape.text());
        // Host 0 took all nine spread calls, wherever the actors landed.
        expect(sampleValue(samples, 'sigx_actors_calls_total', { type: 'Counter' })).toBe(9);
        expect(
            sampleValue(samples, 'sigx_actors_method_calls_total', { type: 'Counter', method: 'increment' })
        ).toBe(9);
        expect(sampleValue(samples, 'sigx_actors_call_duration_seconds_bucket', { le: '+Inf' })).toBe(9);
    });
});

describe('the env knobs', () => {
    it('parsePorts takes exactly three ports and defaults when unset', () => {
        expect(parsePorts(undefined)).toEqual([5591, 5592, 5593]);
        expect(parsePorts(' 6001, 6002 ,6003 ')).toEqual([6001, 6002, 6003]);
        expect(parsePorts('0,0,0')).toEqual([0, 0, 0]);
    });

    it('parsePorts rejects a malformed value with a message that names it', () => {
        for (const bad of ['5591,5592', '5591,5592,5593,5594', '5591,abc,5593', '5591,5592.5,5593', '5591,70000,5593']) {
            expect(() => parsePorts(bad)).toThrow(/PROVIDERS_DEMO_PORTS/);
        }
    });

    it('envOr treats an empty value as unset', () => {
        expect(envOr(undefined, 'dflt')).toBe('dflt');
        expect(envOr('', 'dflt')).toBe('dflt');
        expect(envOr('set', 'dflt')).toBe('set');
    });
});
