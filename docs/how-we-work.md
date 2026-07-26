# How we work in SignalX — the long version

This is the rationale behind the [README](../README.md) TL;DR. The rules exist to
make **many agents and people work the same repo in parallel without stepping on
each other**, and to make every change reviewable and reproducible.

## 1. Branch first, always — the `main`/`branches` layout

Every sigx repo is checked out as:

```
<repo>/
  main/          # primary checkout — build/read here, NEVER edit or commit
  branches/
    <name>/      # one git worktree per branch, deps installed
```

**Why a worktree and not `git switch -c`?** Multiple agent sessions (and you)
share the `<repo>/main` working directory. If one session switches branches or
edits files there, every other session sees the change mid-flight — merge
conflicts, half-applied edits, failed builds. A worktree is a *separate working
directory on its own branch*, so each unit of work is fully isolated, and pnpm
hardlinks deps from the global store so creating one is cheap.

The helper is `scripts/worktree.mjs`, exposed as `pnpm wt`:

```sh
pnpm wt new <name> [--from <branch>]   # <repo>/branches/<name>, new branch, deps installed
pnpm wt list                           # all worktrees, (main) flagged
pnpm wt rm <name> [--force]            # safe remove
```

It refuses non-standard layouts (the primary checkout must be a folder named
`main`), refuses to delete `main` or the worktree you're currently standing in,
validates names as safe git refs, and cleans up the pnpm `node_modules`
junctions on Windows that `git worktree remove` can't.

**The guardrail:** before every commit, `git branch --show-current` must print
your worktree's branch. If it prints `main` or nothing (detached HEAD), stop and
move the work:

```sh
git stash -u
pnpm wt new <N-short-slug>
cd ../branches/<N-short-slug>
git stash pop
```

## 2. The loop: issue → worktree → PR → review → squash-merge

For **agents this is mandatory for every change**, including one-liners. Humans
may skip the issue (`CONTRIBUTING.md`), but not the PR/review/worktree.

- **Issue first** so there's a durable record of *what & why* and a number to
  reference. When you plan in plan mode, the approved plan **is** the issue body —
  no extra writing.
- **Worktree** named `<N-short-slug>` ties the branch to issue `#N`.
- **PR referencing the issue** (`Closes #N`) so it auto-closes on merge, with
  **`@copilot` as reviewer**. Don't merge before Copilot has reviewed; address
  every actionable comment with follow-up commits.
- **Merge yourself** once review is resolved and CI is green: squash, delete the
  branch, `pnpm wt rm` the worktree.

The full commands (including the `gh api` fallback when `@copilot` won't resolve)
are in [`AGENTS.md`](../AGENTS.md).

**Why Copilot review on every PR?** It's a fast, consistent second pair of eyes
that catches the obvious class of mistakes before a human looks, and it makes the
review step a non-negotiable part of the loop rather than an afterthought.

## 3. `main` is protected

Direct pushes are blocked; PR + review + green CI + squash-only are enforced by a
ruleset applied as code. See [branch-protection.md](branch-protection.md). This
is what makes “merge it yourself once it's green” safe — the rules, not
discipline, prevent a bad merge.

## 4. One source of truth for agents

`AGENTS.md` is tool-neutral and canonical. `CLAUDE.md` (and any future
tool-specific file) is a thin shim that `@`-imports it and adds only that tool's
quirks. So there's exactly one place to update the workflow, and every agent —
Claude Code, Copilot CLI, work agents — reads the same rules. When a
tool-specific file conflicts with `AGENTS.md`, it wins *for that tool only*.

## 5. Verify before “done”

**Bug fixes are test-first.** Reproduce the bug with a failing unit test *before*
touching the fix, then make it go green. The red→green transition proves the fix
actually addresses the bug (not just something adjacent), and the test stays
behind as a regression guard so the bug can't silently come back. Never fix a bug
without a test that would have caught it; any missing coverage you stumble on gets
filled in the same PR. CI backs this mechanically: Codecov's `codecov/patch` check
(config in [`codecov.yml`](../codecov.yml)) fails any PR whose changed lines aren't
covered, and it's a required status on `main` — so a fix with no test can't merge.

Code changes run `pnpm typecheck` (always, for any `.ts`) plus the relevant
`pnpm test` / `pnpm build`, with evidence shown. CI re-runs the same gate on
Node 20 + 22 on Linux and Node 22 on Windows — because contributors and CI span
all three OSes, committed scripts are Node, not shell one-liners, and paths use
the running environment's separator.

## 6. Releases are tag-driven

Bump versions → push a `vX.Y.Z` tag → `release.yml` re-runs the full gate and
publishes to npm via **trusted publishing** (OIDC + provenance — no tokens in
CI) and cuts a GitHub Release. `release-drafter` keeps a draft changelog from
Conventional-Commit PR titles, and `dependabot-automerge` lands green patch bumps
on its own. Day to day you never run `npm publish` by hand.

## Working principles (the short list)

- Plan first for non-trivial work; let the CLI manage the plan file.
- Minimal, surgical edits — no unrelated refactors, no compat shims for things
  that never shipped.
- Stage specific files (`git add <path>`), never `git add -A`.
- No co-author trailers on commits.
- Cross-platform by default.
