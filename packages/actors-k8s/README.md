# @sigx/actors-k8s

Kubernetes membership provider for [`@sigx/actors`](../actors) clustering.
Host liveness and peer discovery ride `coordination.k8s.io` **Leases** — the
same primitive kubelet uses for node heartbeats — so a cluster running in
Kubernetes needs no extra store for membership. No Kubernetes client
library either: the API surface this package touches is six verbs on one
resource, spoken in plain HTTPS + JSON.

Membership and the actor **directory** are independent seams; the directory
stays store-backed. Compose them:

```ts
import { createHost } from '@sigx/actors/host';
import { clusterPlacement } from '@sigx/actors/cluster';
import { k8sMembership } from '@sigx/actors-k8s';
import { redisDirectory } from '@sigx/actors-redis';

const host = createHost({
    actors,
    storage,
    placement: clusterPlacement({
        membership: k8sMembership(),
        directory: redisDirectory(redis),
        advertise: `http://${process.env.POD_IP}:7311`,
        secret: process.env.HOST_SECRET
    })
});
```

Inside a pod everything is discovered: API server from the service env vars,
namespace / token / CA from the ServiceAccount mount, with the bound token
re-read on rotation. Call `k8sMembership()` once per host.

## Options

| Option | Default | Meaning |
|---|---|---|
| `namespace` | ServiceAccount namespace, else `default` | where the Leases live |
| `clusterName` | `default` | value of the `sigx.dev/cluster` label — two clusters can share a namespace |
| `labels` | — | extra labels stamped on the own Lease AND selecting peers |
| `leasePrefix` | `sigx` | Lease names are `{leasePrefix}-{hostId}` |
| `heartbeatMs` | `5000` | Lease renewal cadence |
| `ttlMs` | `15000` | liveness TTL, serialized as `spec.leaseDurationSeconds` (ceiled to seconds) — missed renewals past this = dead |
| `clockSkewMs` | `2000` | slack added to peer freshness checks |
| `relistMs` | `60000` | reconciling LIST cadence under the watch; `0` disables |
| `apiServer` | in-cluster env, else `https://kubernetes.default.svc` | API server origin |
| `token` | ServiceAccount token file | bearer token, or a provider function |
| `ca` | ServiceAccount `ca.crt` | PEM bundle for the API server |
| `fetch` | node:https shim | transport override (tests, `kubectl proxy` dev) |
| `watchBackoff` | `{ minMs: 250, maxMs: 5000 }` | watch reconnect backoff bounds |

## How it works

Each host owns one Lease; nobody else ever writes it:

```json
{
    "apiVersion": "coordination.k8s.io/v1",
    "kind": "Lease",
    "metadata": {
        "name": "sigx-s.k3f9a2",
        "labels": { "sigx.dev/cluster": "default" },
        "annotations": { "sigx.dev/descriptor": "{\"hostId\":\"s.k3f9a2\",…}" }
    },
    "spec": {
        "holderIdentity": "s.k3f9a2",
        "leaseDurationSeconds": 15,
        "renewTime": "2026-07-29T12:04:05.123456Z"
    }
}
```

- **Heartbeat** — a merge-patch of `spec.renewTime` every `heartbeatMs`; no
  read-modify-write, no resourceVersion races.
- **Discovery** — every host runs the standard list-then-watch loop over
  Leases matching its labels; a peer is live while its `renewTime` is within
  its own declared duration. Watch events for renewals never bump the
  membership view's version — only descriptor-set changes (join, leave,
  drain, expiry) do, so heartbeats cause zero `onChange` traffic.
- **Self-fencing** — a renewal that fails past `ttlMs` fires
  `onSelfSuspect` once, exactly like the Redis provider: the host stops
  claiming actors and deactivates what it holds. So does one that merely
  *lands* past `ttlMs` — a stalled event loop renews late and succeeds, but
  the Lease went stale while it was away and peers released its claims
  (#45). A broken *watch* never fences — that is staleness, covered by the
  `relistMs` safety net.
- **Graceful exit** — `setStatus('leaving')` patches the descriptor
  immediately (drain is visible now, not next beat); `leave()` deletes the
  Lease. A crashed host simply stops renewing and ages out.
- **Versions** — `view().version` is a local monotonic counter. Kubernetes
  `resourceVersion` is an opaque watch bookmark and is never interpreted.

State integrity never rests on any of this: the actor runtime's storage
etag CAS remains the floor, exactly as with every other provider.

### Clocks

`renewTime` is written by each host's own clock and compared against the
observer's, so peer freshness assumes NTP-synced nodes — the same
assumption kubelet node Leases make. `clockSkewMs` (default 2 s) is the
slack; raise it if your nodes drift more.

### Scale

Every renewal is a watch event delivered to every host: n hosts beating
every 5 s ≈ n²/5 events per second cluster-wide. At tens of hosts this is
trivial (30 hosts ≈ 180 tiny JSON lines/s, and none of them touch the
view). For hundreds of hosts, raise `heartbeatMs`/`ttlMs` — the volume
falls quadratically.

## RBAC

The ServiceAccount needs Lease access in the host namespace, nothing else:

```yaml
apiVersion: rbac.authorization.k8s.io/v1
kind: Role
metadata:
    name: sigx-actors-membership
rules:
    - apiGroups: ["coordination.k8s.io"]
      resources: ["leases"]
      verbs: ["get", "list", "watch", "create", "update", "patch", "delete"]
---
apiVersion: rbac.authorization.k8s.io/v1
kind: RoleBinding
metadata:
    name: sigx-actors-membership
roleRef:
    apiGroup: rbac.authorization.k8s.io
    kind: Role
    name: sigx-actors-membership
subjects:
    - kind: ServiceAccount
      name: my-host
```

## Development outside the cluster

Kubeconfigs are deliberately not parsed (client certificates and exec
plugins are a dependency magnet). Let `kubectl` do the auth instead:

```sh
kubectl proxy --port=8001
```

```ts
k8sMembership({ apiServer: 'http://127.0.0.1:8001', namespace: 'dev' });
```

The proxy authenticates; the provider talks plain localhost HTTP (with no
ServiceAccount mount present, it simply sends no credentials).

## Tests

The provider suite runs against an in-process fake API server — no cluster
needed, `pnpm test actors-k8s`. The real-cluster lifecycle suite is
gated on `KUBECONFIG` and drives a spawned `kubectl proxy`:

```sh
KUBECONFIG=~/.kube/config pnpm test actors-k8s
```
