/**
 * The provider against a REAL Kubernetes API server — env-gated on
 * `KUBECONFIG`. Auth is delegated wholesale to a spawned `kubectl proxy`
 * (client certs, exec plugins, whatever the kubeconfig says), so the
 * provider talks plain localhost HTTP and the package never parses a
 * kubeconfig. Uses the `default` namespace with a per-run `clusterName`,
 * so all it needs is the Lease RBAC from the README.
 */
import { spawn, type ChildProcess } from 'node:child_process';
import { afterAll, beforeAll, describe, expect, it } from 'vitest';
import type { ClusterMembership, HostDescriptor } from '@sigx/actors/cluster';
import { k8sMembership } from '@sigx/actors-k8s';

const KUBECONFIG = process.env.KUBECONFIG;

function descriptor(hostId: string): HostDescriptor {
    return { hostId, epoch: Date.now(), address: `http://${hostId}:7311`, status: 'active' };
}

async function until(cond: () => boolean, ms = 10_000): Promise<void> {
    const start = Date.now();
    while (!cond()) {
        if (Date.now() - start > ms) throw new Error('condition not reached in time');
        await new Promise((resolve) => setTimeout(resolve, 50));
    }
}

describe.skipIf(!KUBECONFIG)('k8s membership over a real API server (kubectl proxy)', () => {
    let proxy: ChildProcess;
    let apiServer = '';

    beforeAll(async () => {
        proxy = spawn('kubectl', ['proxy', '--port=0'], {
            stdio: ['ignore', 'pipe', 'pipe']
        });
        apiServer = await new Promise<string>((resolve, reject) => {
            const timer = setTimeout(() => reject(new Error('kubectl proxy did not start')), 10_000);
            let out = '';
            proxy.stdout!.on('data', (chunk: Buffer) => {
                out += chunk.toString();
                const match = /Starting to serve on (\S+)/.exec(out);
                if (match) {
                    clearTimeout(timer);
                    resolve(`http://${match[1]}`);
                }
            });
            proxy.on('error', reject);
            proxy.on('exit', (code) => reject(new Error(`kubectl proxy exited: ${code}`)));
        });
    }, 15_000);

    afterAll(() => {
        proxy?.kill();
    });

    it('join / converge / drain / leave lifecycle', async () => {
        // Per-run cluster label: leases are isolated even in a shared
        // `default` namespace, and stragglers age out by their own TTL.
        const clusterName = `sigx-test-${Date.now().toString(36)}`;
        // No `fetch` override: this deliberately exercises the package's
        // default node:http(s) transport against a real API server.
        const options = {
            apiServer,
            namespace: 'default',
            clusterName,
            heartbeatMs: 250,
            ttlMs: 2_000
        };
        const a: ClusterMembership = k8sMembership(options);
        const b: ClusterMembership = k8sMembership(options);
        try {
            await a.join(descriptor('s.reala'));
            expect(a.view().hosts.map((s) => s.hostId)).toEqual(['s.reala']);

            await b.join(descriptor('s.realb'));
            await until(() => a.view().hosts.length === 2);
            await until(() => b.view().hosts.length === 2);

            await expect(a.isAlive('s.realb')).resolves.toBe(true);

            await b.setStatus('leaving');
            await until(
                () => a.view().hosts.find((s) => s.hostId === 's.realb')?.status === 'leaving'
            );

            await b.leave();
            await until(() => a.view().hosts.length === 1);
            await expect(a.isAlive('s.realb')).resolves.toBe(false);
        } finally {
            await Promise.allSettled([a.leave(), b.leave()]);
        }
    }, 30_000);
});

describe.runIf(!KUBECONFIG)('k8s membership (no KUBECONFIG)', () => {
    it('skips the real-cluster suite when KUBECONFIG is not set', () => {
        expect(KUBECONFIG).toBeUndefined();
    });
});
