# Contributing to SignalX actors

Thanks for your interest! This repo is part of the
[`signalxjs`](https://github.com/signalxjs) family. It follows the **sigx
standard** working setup — agent guide, git-worktree flow, CI/release pipeline,
and protected `main` — maintained in
[`signalxjs/repo-template`](https://github.com/signalxjs/repo-template).

## Prerequisites

- **Node.js** `^20.19.0` or `>=22.12.0`
- **pnpm** `>=10` (this repo uses workspaces; `npm` and `yarn` are not supported)

## Getting started

We use the standard `main` / `branches` worktree layout. Clone the primary
checkout into a `main` folder:

```bash
git clone https://github.com/signalxjs/actors.git actors/main
cd actors/main
pnpm install
pnpm build
```

> The `build` step is required before tests when packages consume each other's
> `dist/` output through the workspace.

## Working on a change

Create an isolated worktree instead of switching branches in place:

```bash
pnpm wt new my-change      # checkout at <repo>/branches/my-change, deps installed
cd ../branches/my-change
```

See [`AGENTS.md`](./AGENTS.md) for the full worktree flow (it's the same for
people and agents). **Never commit on `main`** — it's protected and shared by
parallel checkouts.

## Common tasks

| Task | Command |
|---|---|
| Build | `pnpm build` |
| Run tests | `pnpm test` |
| Tests in watch mode | `pnpm test:watch` |
| Typecheck | `pnpm typecheck` |
| Lint | `pnpm lint` |
| Check issue references | `pnpm verify:refs` |
| Bundle-size check | `pnpm size` — budgets in `.size-limit.json` track the feature set: a PR that needs a bump ships it, with bytes and why in the PR body; trims are welcome, never required; a bump with no feature behind it is a regression to investigate. `bundle-size.yml` fails a PR that exceeds a limit |
| Benchmarks | `pnpm bench` (see `benchmarks/README.md`) |
| Perf regression check | `pnpm bench:baseline` before your change, `pnpm bench:compare` after |
| Diff two saved bench results | `pnpm bench:diff --before=a.json --after=b.json` |

A PR that touches `packages/**` or `benchmarks/**` also gets a **Bench** comment:
CI measures the base ref and your head ref back to back on one runner and posts
the delta. Two halves, gated differently:

- **Timings never fail the check** — shared runners are too noisy for that. Treat
  them as a pointer and reproduce anything interesting locally on a quiet machine.
- **Exact metrics do fail it.** Those are invariants (directory calls per
  activation, microtask turns per dispatch), identical on every machine, so a red
  Bench check means real behaviour changed. If the change is intended, say so in
  the PR and update the expectation.

## Pre-push checklist

Before opening a PR, run:

```bash
pnpm lint
pnpm typecheck
pnpm test
pnpm build
```

## Pull request guidelines

- **Keep PRs small and focused.** One logical change per PR.
- **Reference an issue** if one exists; otherwise describe the motivation in the PR body.
- **Add tests.** New behaviour gets tests. **Fix bugs test-first:** add a unit test that *fails* because of the bug, then fix until it's green (red → green). If you spot behaviour that should be covered but isn't, add the missing tests in the same PR.
- **Update `CHANGELOG.md`** under the `[Unreleased]` section (for user-visible changes).
- **Update docs in the same PR.** Package/script/API changes update the in-repo docs (see `AGENTS.md` → Documentation); user-facing changes also need an issue filed on the docs repo [`signalxjs/signalxjs.github.io`](https://github.com/signalxjs/signalxjs.github.io) before merge, linked from the PR body — the docs agent picks it up from there (don't open docs-site PRs yourself).
- **Don't bump versions** in your PR — releases are handled centrally via tags.
- **Squash-merge only.** `main` rejects merge commits; CI must be green and the
  PR reviewed before merge.

## Reporting bugs and requesting features

- **Bug?** Open an issue with the [bug report template](https://github.com/signalxjs/actors/issues/new?template=bug_report.yml). A minimal reproduction helps a lot.
- **Feature idea?** Use the [feature request template](https://github.com/signalxjs/actors/issues/new?template=feature_request.yml).

## Code of conduct

This project follows the [Contributor Covenant](./CODE_OF_CONDUCT.md). Be kind.

## License

By contributing, you agree that your contributions will be licensed under the
MIT License (see [LICENSE](./LICENSE)).
