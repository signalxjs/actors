/** @jsxImportSource @sigx/terminal */
/**
 * Prints each screen so a human can LOOK at it.
 *
 * Not an assertion — a development aid, skipped by default. The bug this
 * whole rebuild fixes shipped because nobody ever saw the output: the
 * assertions matched content and the screenshots came months later.
 *
 *     npx vitest run packages/actors-cli/__tests__/eyeball.test.tsx -t print
 */
import { describe, it } from 'vitest';
import { render, renderNodeToLines } from '@sigx/terminal';
import {
    ClusterScreen,
    GrainsScreen,
    HealthScreen,
    OverviewScreen,
    SilosScreen
} from '../src/dashboard/screens';
import { DashboardState } from '../src/dashboard/state';
import type { MonitorSnapshot, MonitorSource, SiloView } from '../src/source/types';

const stats = {
    activations: 30,
    queued: 2,
    perType: { Counter: 30 },
    transitional: { activating: 0, deactivating: 0 }
};

const silo = (over: Partial<SiloView> = {}): SiloView => ({
    siloId: 's.2sme5hx2',
    address: 'http://127.0.0.1:5391',
    status: 'active',
    uptimeMs: 142_000,
    stats,
    counters: null,
    reminderShards: ['p0'],
    membershipVersion: 5,
    transports: ['http'],
    ...over
});

const hist = (p99: number) => ({
    count: 120,
    minMs: 0,
    maxMs: p99,
    meanMs: p99 / 4,
    p50Ms: p99 / 8,
    p90Ms: p99 / 2,
    p99Ms: p99
});

const snapshot: MonitorSnapshot = {
    at: 1_700_000_000_000,
    silos: [silo(), silo({ siloId: 's.ikfugf49', status: 'leaving', stats: { ...stats, activations: 2, queued: 0 } })],
    cluster: {
        from: 's.2sme5hx2',
        view: { version: 5, size: 2, active: 1 },
        totals: {
            silos: 2,
            activations: 32,
            queued: 2,
            perType: { Counter: 32 },
            counters: {
                routedLocal: 1280, remoteDispatches: 1280, remoteStreams: 1, remoteWatches: 0, retries: 15,
                routingFailures: 0, inboundDispatches: 1280, inboundStreams: 0, inboundWatches: 0,
                routeCacheHits: 238, routeCacheMisses: 44, directoryLookups: 44,
                directoryClaims: 37, claimConflicts: 4, directoryReleases: 0,
                directoryEvictions: 0, siloSweeps: 2, sweptEntries: 4,
                wrongHostRedirects: 3, unreachableRetries: 0, drainingRetries: 0,
                authFailures: 0, transportFallbacks: 0, membershipChanges: 5,
                selfFences: 0, claimed: 32, routeCacheSize: 39, locates: 44, locateRemote: 12
            }
        },
        reminderShards: Object.fromEntries(
            Array.from({ length: 16 }, (_, i) => [`p${i}`, i === 3 ? [] : ['s.2sme5hx2']])
        ),
        unreachable: []
    },
    metrics: {
        windowMs: 1000,
        calls: { total: 593, failed: 26, streams: 0 },
        latencyMs: hist(608 / 1000),
        queueMs: hist(272 / 1000),
        turnMs: hist(320 / 1000),
        byType: {},
        byMethod: {
            'Counter#increment': { calls: 989, failed: 0, latencyMs: null, queueMs: null, turnMs: hist(0.216) },
            'Counter#nope': { calls: 47, failed: 47, latencyMs: null, queueMs: null, turnMs: hist(0.136) }
        },
        errors: {
            byKind: { 'method-not-found': 68 },
            recent: [
                { at: 1_700_000_000_000, type: 'Counter', method: 'nope', kind: 'method-not-found', message: 'actor type "Counter" has no method "nope"' }
            ]
        },
        activations: { created: 40, destroyed: 8, byReason: { idle: 8 } },
        storage: { loads: 158, saves: 2860, clears: 0, conflicts: 0, latencyMs: hist(0.4) },
        gauges: null
    },
    activations: [
        { type: 'Counter', key: 'cart', queued: 0, ageMs: 167_000, idleMs: 167_000, keptAlive: false, tasks: 0 },
        { type: 'Counter', key: 'cold-0', queued: 0, ageMs: 161_000, idleMs: 2000, keptAlive: false, tasks: 0 },
        { type: 'Counter', key: 'cold-1', queued: 3, ageMs: 167_000, idleMs: 2000, keptAlive: true, tasks: 2 }
    ],
    health: {
        live: true,
        ready: true,
        fatal: false,
        uptimeMs: 242_000,
        checks: { cluster: { ready: true, detail: 'active' } },
        silo: null
    },
    partial: false
};

const inert: MonitorSource = {
    kind: 'http',
    label: 'http://127.0.0.1:5391',
    snapshot: () => Promise.reject(new Error('unused')),
    close: () => Promise.resolve()
};

function draw(node: unknown): string[] {
    const container = { type: 'element', tag: 'box', props: {}, children: [] } as never;
    render(node as never, container);
    return renderNodeToLines(container);
}

describe('eyeball', () => {
    it.skip('print', () => {
        const state = new DashboardState({ source: inert });
        state.view.snapshot = snapshot;
        state.view.lastOk = snapshot.at;
        for (let i = 0; i < 24; i++) {
            state.calls.push(i % 7 === 0 ? null : 400 + Math.round(Math.sin(i) * 180));
            state.failures.push(i % 5 === 0 ? 6 : 2);
            state.queued.push(i % 3);
            state.activations.push(30 + (i % 4));
        }
        const screens: [string, unknown][] = [
            ['OVERVIEW', <OverviewScreen state={state} />],
            ['SILOS', <SilosScreen state={state} cursor={0} width={78} />],
            ['GRAINS', <GrainsScreen state={state} cursor={1} width={78} />],
            ['CLUSTER', <ClusterScreen state={state} />],
            ['HEALTH', <HealthScreen state={state} />]
        ];
        for (const [name, node] of screens) {
            const out = draw(node);
            console.log(`\n${'═'.repeat(70)}\n  ${name}  (${out.length} lines)\n${'═'.repeat(70)}`);
            for (const line of out) console.log(line);
        }
    });
});
