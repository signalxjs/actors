# Adopting the sigx standard in a repo

Two paths: a **new repo** (use the GitHub template) or an **existing repo**
(copy the files in). Both end with the same one-time branch-protection step.

---

## New repo — “Use this template”

1. On <https://github.com/signalxjs/repo-template>, click **“Use this template” →
   “Create a new repository”**. Name it, owner `signalxjs`.
2. Clone it into the standard layout (note the `/main` folder):
   ```sh
   git clone https://github.com/signalxjs/actors.git actors/main
   cd actors/main
   ```
3. Run the **customization checklist** below.
4. Lock down `main` (see [branch-protection.md](branch-protection.md)):
   ```sh
   node scripts/apply-branch-protection.mjs signalxjs/actors
   ```

---

## Existing repo — copy the files in

Copy these from this template into the repo (keep the **verbatim** ones byte-for-byte):

| File / dir | Notes |
|---|---|
| `scripts/worktree.mjs` | **verbatim** |
| `scripts/apply-branch-protection.mjs` | **verbatim** |
| `scripts/sync-core.mjs` | **verbatim** (core catalog alignment) |
| `scripts/check-catalog.mjs` | **verbatim** (`verify:catalog` guard) |
| `scripts/lib/core-deps.mjs` | **verbatim** — the shared core-package list both of the above import. Copy it or neither script runs |
| `CLAUDE.md` | **verbatim** |
| `AGENTS.md` | template — edit the repo-specific sections |
| `.github/workflows/*` | copy what applies (see “trimming” below) |
| `.github/` (CODEOWNERS, dependabot, templates, SUPPORT) | edit owners + package lists |
| `CONTRIBUTING.md`, `SECURITY.md`, `CODE_OF_CONDUCT.md` | edit repo name |
| `.npmrc`, `.gitignore`, `.size-limit.json` | edit scope/paths |

Then add to the repo’s `package.json` scripts:

```json
{
  "scripts": {
    "wt": "node scripts/worktree.mjs",
    "branch-protection": "node scripts/apply-branch-protection.mjs",
    "sync:core": "node scripts/sync-core.mjs",
    "verify:catalog": "node scripts/check-catalog.mjs"
  }
}
```

Re-clone into the `<repo>/main` layout if it isn’t already (the worktree helper
requires the primary checkout to live in a folder named `main`).

---

## Customization checklist

Search the repo for the two markers and resolve each:

```sh
# every placeholder that must become the repo name:
git grep -n "actors"
# every spot that needs a per-repo decision:
git grep -n "TODO(sigx-standard)"
```

PowerShell find-and-replace for the repo name (run from the repo root):

```powershell
Get-ChildItem -Recurse -File -Include *.md,*.yml,*.json |
  ForEach-Object {
    (Get-Content $_ -Raw) -replace 'actors', 'core' | Set-Content $_ -NoNewline
  }
```

Then, file by file:

- [ ] **`AGENTS.md`** — rewrite the intro paragraph (what this repo is), the
      “Build, Test, Lint” commands, and the “Packages” list. Single-package
      repo? Drop the workspace/`--filter` bits and the “Packages” section.
- [ ] **`.github/CODEOWNERS`** — set the real owner(s) (default is `@andtii`).
- [ ] **`.github/ISSUE_TEMPLATE/bug_report.yml` + `feature_request.yml`** —
      fill the package dropdowns, or delete them for a single package.
- [ ] **`.size-limit.json`** — set real dist paths and limits, or delete the
      file and `bundle-size.yml` if you don’t ship a bundle.
- [ ] **`SECURITY.md`** — set the supported-versions line.
- [ ] **`package.json`** — `name`, `description`, `repository`, plus `lint` /
      `typecheck` / `build` / `test` / `size` scripts that `AGENTS.md` and CI call.
- [ ] **`codecov.yml`** — keep as-is to enforce patch coverage, or tune the
      `patch` target. See "Enable Codecov coverage gating" below to turn it on.

### Enable Codecov coverage gating

This is what mechanically enforces the test-first convention — a PR whose changed
lines aren't tested fails the `codecov/patch` check. To turn it on:

1. Install the **[Codecov GitHub App](https://github.com/apps/codecov)** on the
   `signalxjs` org (or this repo) so Codecov can post checks.
2. Add the repo on [codecov.io](https://about.codecov.io/) and copy its upload
   token into the repo's `CODECOV_TOKEN` Actions secret
   (`gh secret set CODECOV_TOKEN -R signalxjs/actors`). The `coverage` job in
   `ci.yml` already uploads using it.
3. Make `codecov/patch` a **required status check** on `main` once it's reporting:
   ```sh
   node scripts/apply-branch-protection.mjs signalxjs/actors \
     --checks "test (ubuntu-latest, 22); verify-pack; codecov/patch"
   ```
   (`codecov.yml` keeps `codecov/project` informational, so only the patch gate
   blocks.)

### Trimming workflows per repo

- `ci.yml` — keep always. Don't use Codecov? Drop the `coverage` job *and*
  `codecov.yml`. Drop `verify-pack` if you don't publish.
- `bundle-size.yml` — keep only if you ship a size-limited bundle.
- `release.yml` — keep only if you publish to npm; needs `scripts/publish.js` +
  trusted publishing configured on npmjs.com. See its header comment.
- `core-sync.yml` — keep for any repo that consumes sigx core. It needs the
  `catalog:` block set up (below) and `verify:catalog` wired into `ci.yml`. Works
  standalone via the weekly cron + manual dispatch; the instant `core-released`
  path also needs the core-side token (below).
- `release-drafter.yml`, `dependabot-automerge.yml` — keep for any repo.

### Wire up core catalog alignment

For any repo that consumes sigx core (`@sigx/reactivity` et al.):

1. Add a `catalog:` block to `pnpm-workspace.yaml` pinning every core package this
   repo uses to a single minor, e.g.:
   ```yaml
   catalog:
     "@sigx/reactivity": ^0.12.0
     "@sigx/runtime-core": ^0.12.0
     sigx: ^0.12.0
   ```
   Then replace the version of every core dep in each package.json
   (`dependencies` / `devDependencies` / `peerDependencies`) with `"catalog:"`.

   **Step 1 is not optional and cannot be skipped.** `sync:core` edits the catalog
   and nothing else, so a repo whose core deps are still pinned inline has nothing
   for it to rewrite. It refuses to run in that state and names each offending
   specifier:

   ```
   sync-core: this repo pins core packages INLINE, outside the catalog:
     - @acme/thing dependencies["@sigx/reactivity"] = "^0.12.0" (must be "catalog:")
   ```

   (It used to report "already aligned" and exit 0 there — a false green that
   `core-sync.yml` swallowed as success while the repo sat on the old core.)
2. Add the `sync:core` + `verify:catalog` scripts (shown above) and keep the
   **Verify catalog** step in `ci.yml`. Run `pnpm verify:catalog` locally to
   confirm it's green.
3. Keep `.github/workflows/core-sync.yml`. To make alignment PRs land *instantly*
   on a core release (not just weekly), the core repo must dispatch a
   `core-released` event to this repo — see "Activating the instant dispatch"
   below.

### Activating the instant dispatch (org-wide, one-time)

`core-sync.yml` already listens for a `core-released` `repository_dispatch`. The
sender lives in `signalxjs/core`'s `release.yml` (`notify-consumers` job). To turn
the fast path on:

1. Create a fine-grained PAT at
   <https://github.com/settings/personal-access-tokens/new> with **resource owner
   `signalxjs`** (not a personal account, or the repos will not be listable),
   *Only select repositories* → every consumer repo, and **`Contents: Read and
   write`** — nothing else (`Metadata: Read` is added automatically).

   **There is no "Repository dispatch" permission.**
   `POST /repos/{owner}/{repo}/dispatches` requires `Contents: Read and write`, which is
   broader than the job needs and the narrowest GitHub offers for it (a GitHub App
   needs the same). Prefer a short expiry over hunting for a tighter scope — and
   note the expiry, since core's job swallows per-repo dispatch failures, so an
   expired token degrades silently to the weekly cron.
2. Store it as the **`ECOSYSTEM_DISPATCH_TOKEN`** secret **in the core repo**
   (`gh secret set ECOSYSTEM_DISPATCH_TOKEN -R signalxjs/core`).
3. Ensure each consumer's `core-sync.yml` is on `main` (this template ships it) and
   the repo is listed in core's fan-out loop.

Until that's done, `core-sync.yml` still catches new core releases weekly via cron.

---

## Verify it works

```sh
pnpm install
pnpm wt new smoke-test        # creates <repo>/branches/smoke-test
pnpm wt list                  # main is flagged
pnpm wt rm smoke-test         # cleans up
node scripts/apply-branch-protection.mjs signalxjs/actors --dry-run
```

Open a throwaway PR and confirm CI runs and `main` rejects a direct push.
