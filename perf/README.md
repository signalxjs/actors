# perf — the performance and deployment rig

**Nothing in here is an example.** It is the harness the repo measures
itself with: a real cloud deployment, driven by a real load generator, so
that claims about throughput, locality and failover are backed by something
other than a diagram.

If you are looking for code to copy, go to [`examples/`](../examples/)
instead. The rig optimises for the opposite things an example does — knobs
over defaults, instrumentation over clarity, and behaviour that can be
broken on purpose.

| | |
|---|---|
| [`aks/`](aks/) | The AKS estate: `deploy/testenv.mjs` (one command per verb — up / status / test / baseline / bench / load / migrate-check / down), the Helm chart, the runbook, an in-cluster load generator, and the env-gated deployment suite |

## Where the numbers go

The scenarios themselves live in `benchmarks/src/scenarios/infra.ts` — the
**Tier 3** tier, gated behind `BENCH_INFRA=1` plus the estate env vars and
skipped otherwise. `benchmarks/BASELINES.md` records what was measured, and
its tier legend exists so a modelled figure is never quoted as a measured
one.

Two rules the rig is built around, both learned the hard way:

- **A shared runner cannot judge a timing.** Tier 3 is not run in CI on
  every PR for that reason; it is run deliberately, from a same-region VM,
  against a deployment that exists for the length of the session.
- **The estate is not public even though this code is.** Identity values
  (resource group, cluster, registry, region, DNS zone, load-VM names) have
  no defaults anywhere in this tree. A verb fails fast naming exactly what
  it is missing; in CI the values come from Actions secrets. `perf/aks`'s
  `__tests__/testenv-config.test.ts` runs on every `pnpm test` to keep it
  that way.

## Cost

`testenv.mjs up` creates billable Azure resources — a node pool, two
container images, a load VM and a DNS record. `down` removes all of it.
`status` flags the load VM explicitly, because an idle VM is the thing
everyone forgets.
