# repo-template — how we work in SignalX

> The canonical engineering setup for every repo under
> [`signalxjs`](https://github.com/signalxjs). This is a **GitHub template
> repository**: click **“Use this template”** to start a new repo with the
> agent workflow, git-worktree flow, CI/release pipeline, branch protection,
> and governance docs already wired up. Existing repos adopt it by copying the
> files in (see [`docs/adopting.md`](docs/adopting.md)).

It captures the patterns first proven in
[`signalxjs/core`](https://github.com/signalxjs/core) and makes them portable.

---

## TL;DR — the rules that matter

1. **Branch first. Never work on `main`.** Every change — however small — happens
   in a git worktree at `<repo>/branches/<name>`, never in the primary checkout
   at `<repo>/main`. See [the worktree flow](#git-worktrees--the-mainbranches-layout).
2. **Issue → worktree → PR → review → squash-merge.** This is the whole loop.
   Agents make it mandatory (even for one-liners); humans may skip the issue.
   See [the development workflow](#the-development-workflow).
3. **`main` is protected.** No direct pushes, PR + green CI + review required,
   squash-only. Enforced as code by
   [`scripts/apply-branch-protection.mjs`](scripts/apply-branch-protection.mjs).
4. **Agents read `AGENTS.md`.** It is the single, tool-neutral source of truth.
   `CLAUDE.md` (and any future tool file) is a thin shim that `@`-imports it.
5. **Verify before “done.”** Typecheck + test + build, with evidence, before any
   PR is merged. CI re-checks the same commands on Linux + Windows.

---

## What’s in this template

| Path | What it is | Portable as-is? |
|---|---|---|
| [`AGENTS.md`](AGENTS.md) | Tool-neutral agent guide: the workflow, build/test, conventions | Edit the repo-specific top + “Build/Test” + “Packages” sections |
| [`CLAUDE.md`](CLAUDE.md) | Thin Claude Code shim that imports `AGENTS.md` | **Verbatim** |
| [`scripts/worktree.mjs`](scripts/worktree.mjs) | `pnpm wt new/list/rm` — the worktree helper | **Verbatim** |
| [`scripts/apply-branch-protection.mjs`](scripts/apply-branch-protection.mjs) | Applies the `main` ruleset via `gh api` | **Verbatim** (pass the repo) |
| [`scripts/sync-core.mjs`](scripts/sync-core.mjs) | `pnpm sync:core` — aligns the `catalog:` core pins to a core version (`--check` drift guard) | **Verbatim** |
| [`scripts/check-catalog.mjs`](scripts/check-catalog.mjs) | `pnpm verify:catalog` — guards the single-minor catalog invariant (CI + local) | **Verbatim** |
| [`scripts/lib/core-deps.mjs`](scripts/lib/core-deps.mjs) | The core-package list + inline-pin scan the two scripts above share | **Verbatim** — copy it or neither runs |
| [`.github/workflows/`](.github/workflows) | CI, bundle-size, release, release-drafter, dependabot auto-merge, core-sync | Mostly verbatim; trim per repo |
| [`.github/`](.github) | CODEOWNERS, dependabot, PR + issue templates, SUPPORT | Edit owners, package lists |
| `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md` | Governance | Edit repo name/scope |
| [`.npmrc`](.npmrc), [`.gitignore`](.gitignore), [`.size-limit.json`](.size-limit.json) | Workspace config | Edit paths/scope |
| [`docs/`](docs) | The long-form handbook (workflow, branch rules, adopting) | Reference |

Anything marked **verbatim** should be copied unchanged so it stays identical
across repos — that’s what makes the muscle memory transfer between projects.
Placeholders that must change per repo are written as `actors` (the repo name)
or flagged with a `TODO(sigx-standard)` comment.

---

## The development workflow

`issue → worktree → PR → Copilot review → squash-merge`. This is mandatory for
**every agent-driven change**, including one-line fixes. Human contributors may
skip the issue (see `CONTRIBUTING.md`); the worktree + PR + review are not
optional for anyone.

1. **Issue first.** No tracking issue yet? Create one *before* writing code, and
   put the plan in the body. If you worked in plan mode, the approved plan **is**
   the issue body.
   ```sh
   gh issue create --title "<concise title>" --body "<what & why + the plan>"
   ```
2. **Worktree, always.** `pnpm wt new <N-short-slug>` (where `N` is the issue
   number) gives an isolated checkout on branch `<N-short-slug>` with deps
   installed. Never `git switch -c` in `<repo>/main` — parallel sessions share it.
3. **Implement & verify.** Make the change, then prove it: `pnpm typecheck`
   (always, for any `.ts`) plus the relevant `pnpm test` / `pnpm build`. Stage
   specific files (`git add <path>`), never `git add -A`. No co-author trailers.
4. **Open a PR with Copilot as reviewer.** Reference the issue so it auto-closes:
   ```sh
   gh pr create --base main --title "<title>" \
     --body "Closes #N. <short summary>" --reviewer @copilot
   ```
5. **Wait for the review, then fix.** Don’t merge before Copilot has reviewed.
   Address every actionable comment with follow-up commits; re-request review
   until there’s no remaining feedback.
6. **Merge it yourself** once review is resolved **and** CI is green:
   ```sh
   gh pr checks <pr>                          # all green first
   gh pr merge <pr> --squash --delete-branch
   pnpm wt rm <name>                          # clean up the worktree
   ```

The full version, including the `gh api` fallback when `@copilot` won’t resolve,
lives in [`AGENTS.md`](AGENTS.md) and [`docs/how-we-work.md`](docs/how-we-work.md).

---

## Git worktrees — the `main`/`branches` layout

Every sigx repo is checked out as:

```
<repo>/
  main/                 # the primary checkout — read/build here, never edit
  branches/
    <name>/             # one worktree per branch, deps installed
    <other>/
```

`scripts/worktree.mjs` (exposed as `pnpm wt`) is the only tool you need:

```sh
pnpm wt new <name> [--from <branch>]   # new worktree at <repo>/branches/<name>
pnpm wt list                           # show all worktrees ((main) is flagged)
pnpm wt rm <name> [--force]            # remove one (won't touch main or your cwd)
```

Each worktree is an independent checkout, so you can launch a **separate agent
session per directory** and work several things in parallel without ever
switching branches in place. The script enforces the `main` layout, refuses to
delete the checkout you’re standing in, and cleans up pnpm’s Windows
node_modules junctions that `git worktree remove` chokes on.

---

## Branch protection (`main` is protected)

Branch rules live in GitHub settings, not files — so we keep them **as code**.
Run once per repo (needs `gh auth login` with admin on the repo):

```sh
node scripts/apply-branch-protection.mjs signalxjs/actors
```

It applies a repository **ruleset** that requires, on `main`:

- **No direct pushes** — all changes land via PR.
- **A pull request** with at least one approving review, stale approvals
  dismissed on new commits, and CODEOWNERS review where applicable.
- **Required status checks green** — the CI jobs (lint, typecheck, build, test)
  must pass before merge.
- **Squash-only merges** — linear history; merge commits are blocked.
- **No force-push, no deletion** of `main`.

See [`docs/branch-protection.md`](docs/branch-protection.md) for the exact
ruleset and the reasoning.

---

## CI / release pipeline

| Workflow | Trigger | Does |
|---|---|---|
| `ci.yml` | PR + push to `main` | Lint → **verify:catalog** → typecheck → build → test on Node 20/22 (Linux) + 22 (Windows); pack-verify; coverage → Codecov |
| `core-sync.yml` | `core-released` dispatch, weekly cron, manual | Aligns the `catalog:` core pins to the new core version, proves it builds/tests green, then opens/updates a **“chore: align with core”** PR |
| `codecov.yml` | (config) | **Patch-coverage gate** — `codecov/patch` fails a PR whose changed lines aren't tested, enforcing the test-first convention. Project coverage stays informational |
| `bundle-size.yml` | PR | `size-limit` check, comments the delta on the PR |
| `release-drafter.yml` | push/PR to `main` | Maintains a draft release + auto-labels PRs from Conventional-Commit titles |
| `release.yml` | tag `v*.*.*` | Re-runs the full gate, then npm **trusted publishing** (OIDC, provenance) + GitHub Release |
| `dependabot-automerge.yml` | Dependabot PRs | Auto-merges green patch-level dependency bumps |

Releasing is tag-driven: bump versions → push a `vX.Y.Z` tag → `release.yml`
publishes. No tokens in CI for npm — it uses OIDC trusted publishing, configured
per-package on npmjs.com.

---

## Staying aligned with sigx core

Every sigx repo consumes core packages (`@sigx/reactivity`, `@sigx/runtime-core`,
`@sigx/runtime-dom`, `@sigx/server-renderer`, `@sigx/server`, `@sigx/ssr-islands`,
`@sigx/resume`, `@sigx/cache`, `@sigx/vite`, `sigx`). Because core keeps reactive
state in module-local variables, **exactly one physical copy must resolve** — two
copies silently break reactivity. The guarantee: pin every core package to a
**single minor** (`^X.Y.0` == `>=X.Y.0 <X.(Y+1).0`) so pnpm hoists one copy, and
declare that pin **once** — in the `catalog:` block of `pnpm-workspace.yaml`. Every
package.json then references it as `"catalog:"`.

Three pieces keep that invariant honest and make a core bump a one-line, automated
PR:

| Piece | What it does |
|---|---|
| [`scripts/check-catalog.mjs`](scripts/check-catalog.mjs) (`pnpm verify:catalog`) | Fails CI if any package.json declares a core dep with an **inline** version instead of `"catalog:"`, or if a catalog core entry isn't a single-minor `^X.Y.0`. Wired into `ci.yml`. |
| [`scripts/sync-core.mjs`](scripts/sync-core.mjs) (`pnpm sync:core [version]`) | Rewrites the catalog's **core** entries to `^X.Y.0` (siblings like `@sigx/router` are left alone). No arg = latest on npm. `--check` exits non-zero if a change *would* be made (drift guard). Refuses to run — non-zero, naming each specifier — in a repo whose core deps are still pinned inline, since it edits only the catalog and would otherwise report a false "already aligned". |
| [`scripts/lib/core-deps.mjs`](scripts/lib/core-deps.mjs) | The single `CORE_PACKAGES` list and the shared package.json scan. **Keep it in sync with `corePackages` in core's [`docs/ecosystem.json`](https://github.com/signalxjs/core/blob/main/docs/ecosystem.json)** — core's CI fails when a newly published package is missing there, and that failure is the cue to update this list too. A core package absent from here gets neither the rewrite nor the guard, silently. |
| [`.github/workflows/core-sync.yml`](.github/workflows/core-sync.yml) | Runs `sync:core`, then `install → build → typecheck → test → verify:catalog`, and only if all green opens/updates a **“chore: align with core”** PR. A red PR never appears: if a core release breaks this repo, the workflow fails loudly instead. |

### Closing the loop (core → consumers)

`core-sync.yml` triggers three ways:

- **`repository_dispatch` (`core-released`)** — the fast path. `signalxjs/core`’s
  `release.yml` fans out a `core-released` event to every consumer right after it
  publishes, so alignment PRs appear within minutes of a core release.
- **`schedule`** — a weekly Monday cron as a safety net for any dispatch that was
  missed (token expired, workflow absent, …).
- **`workflow_dispatch`** — manual, with an optional target version.

**The dispatch side lives in core, and needs a token.** The default `GITHUB_TOKEN`
cannot dispatch to *other* repos, so core’s `notify-consumers` job authenticates
with an **`ECOSYSTEM_DISPATCH_TOKEN`** secret — a fine-grained PAT whose
**resource owner is the org** (not a personal account) granting **`Contents: Read
and write`** on each consumer repo. There is no "Repository dispatch" permission —
`POST /repos/{owner}/{repo}/dispatches` requires `Contents: Read and write`, which
is broader than the job needs and the narrowest GitHub offers for it. See the companion
change in [`signalxjs/core`](https://github.com/signalxjs/core) that adds that job
to `release.yml`. Until that token is created and that job merged, `core-sync.yml`
still works via the weekly cron and manual dispatch — the dispatch just makes it
instant.

---

## Adopting this in a repo

- **New repo:** click **“Use this template”** on GitHub, then follow the
  customization checklist in [`docs/adopting.md`](docs/adopting.md).
- **Existing repo:** copy the files in per [`docs/adopting.md`](docs/adopting.md).

After either, run `node scripts/apply-branch-protection.mjs signalxjs/actors`
once to lock down `main`.

---

## Suggestions / roadmap

A few improvements worth considering (and tracked as issues here):

- **An `init` script** that copies the standard into an existing repo and patches
  its `package.json` in one command (degit-style). Deferred for now — the
  template + checklist covers the common case.
- **An org-level `signalxjs/.github` repo** for community-health *defaults*
  (CONTRIBUTING/SECURITY/issue-templates) so repos that don’t override them
  inherit automatically. Complements this template rather than replacing it.
- **A `renovate`/dependabot grouping convention** shared across repos.
- **Reusable workflows** (`workflow_call`) so `ci.yml` is one line per repo
  instead of a copied file — reduces drift but adds a moving part.

See [`docs/how-we-work.md`](docs/how-we-work.md) for the rationale behind each
convention.
