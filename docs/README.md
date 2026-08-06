# Repository documentation

Two kinds of document live here, and it is worth knowing which you are reading.

**The user manual is not in this repository.** Guides, API reference, clustering
and deployment all live on the docs site:

> **https://sigx.dev/actors**

Package READMEs point there deliberately: one manual, maintained in one place.
The docs site is built from
[`signalxjs/signalxjs.github.io`](https://github.com/signalxjs/signalxjs.github.io)
and is **not** edited from this repo — see the "Documentation" section of
[`AGENTS.md`](../AGENTS.md) for how changes reach it.

## Architecture notes — [`architecture/`](architecture)

How the runtime is put together, for people changing it. These describe seams,
invariants and the reasoning behind them; they move with the code and are
allowed to, which is exactly why they are here rather than on the site.

| | |
|---|---|
| [`architecture/README.md`](architecture/README.md) | Map — which seam, which file |
| [`architecture/seams.md`](architecture/seams.md) | Every extension point: contract, implementers, invariants |
| [`architecture/runtime-internals.md`](architecture/runtime-internals.md) | Activation, turns, what runs outside one, deadlock detection |
| [`architecture/wire-and-frames.md`](architecture/wire-and-frames.md) | Mounts, envelope, reserved names, the frame codec |
| [`architecture/clustering.md`](architecture/clustering.md) | Membership, directory, placement resolution, rebalancing |
| [`architecture/conformance-suites.md`](architecture/conformance-suites.md) | The workspace-only suites every provider runs |

## Process

| | |
|---|---|
| [`how-we-work.md`](how-we-work.md) | The engineering loop: branch, worktree, PR, review, squash |
| [`branch-protection.md`](branch-protection.md) | `main` protection as code |

## Recipes

| | |
|---|---|
| [`job-recipes.md`](job-recipes.md) | Shapes for durable jobs: cron on reminders, queue-worker actors, Cloudflare checkpointing |
