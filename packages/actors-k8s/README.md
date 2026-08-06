# @sigx/actors-k8s

Kubernetes membership provider for [`@sigx/actors`](https://sigx.dev/actors)
clustering. Host liveness and peer discovery ride `coordination.k8s.io`
**Leases** — the same primitive kubelet uses for node heartbeats — so a cluster
running in Kubernetes needs no extra store for membership.

- **`k8sMembership()`** — one Lease per host, renewed on the heartbeat cadence,
  with a label-selected Lease watch feeding the membership view.

No Kubernetes client library: the API surface this package touches is six verbs
on one resource, spoken in plain HTTPS + JSON. Zero runtime dependencies.

Membership and the actor **directory** are independent seams — the directory
stays store-backed, so compose this with (for example) `redisDirectory()`.

```sh
pnpm add @sigx/actors-k8s
```

Node-only (`node:https`, `node:fs`). `@sigx/actors` is a peer dependency. The
ServiceAccount needs `get`/`list`/`watch`/`create`/`update`/`patch`/`delete` on
`leases` in the host namespace, and nothing else.

## Documentation

**https://sigx.dev/actors/packages/actors-k8s/overview/**

Kubernetes guide: https://sigx.dev/actors/docs/kubernetes/ ·
Clustering guide: https://sigx.dev/actors/docs/clustering/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
