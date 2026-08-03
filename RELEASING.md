# Releasing

All nine `@sigx/actors*` packages release together, on one version, from one
tag. Publishing is done by CI over npm **trusted publishing** (OIDC) — there is
no npm token in this repo, and no workflow references one.

## One-time setup

Trusted publishing is configured per package on npmjs.com, and it binds to an
exact `owner/repo` **and** workflow path. For each of the nine packages, set:

- Repository: `signalxjs/actors`
- Workflow: `.github/workflows/release.yml`

A run from any other repository is rejected at token exchange, so this has to
be redone if the repo ever moves.

## Cutting a release

1. **Land everything first.** The tag is cut from `main`; nothing is published
   from a branch.

2. **Move the versions.**

   ```sh
   pnpm version:minor          # or version:patch / version:major
   pnpm version:set 0.2.0      # …or an exact version
   ```

   This rewrites `version` in all nine manifests **and** the cross-package
   `peerDependencies` ranges that point at them. Both halves matter: the eight
   sibling packages peer on `@sigx/actors`, and publishing them against a stale
   range means demanding a version that is no longer `latest`.

3. **Move the changelogs.** In each package that changed, rename the
   `[Unreleased]` heading to the new version with today's date, and open a
   fresh `[Unreleased]` above it.

4. **Verify locally.**

   ```sh
   pnpm typecheck && pnpm lint && pnpm test && pnpm build
   pnpm verify:pack            # packs all nine, installs them, import-smokes each entry
   pnpm publish:dry
   ```

   `verify:pack` is the one that catches packaging mistakes nothing else sees —
   a missing `files` entry, a broken `exports` map, a stale `dist/`, or a
   tarball with no LICENSE in it.

5. **Commit, then tag.** The tag must match the manifests exactly; the release
   workflow refuses to publish otherwise, because `publish.js` skips versions
   already on the registry and would otherwise produce an empty release.

   ```sh
   git commit -am "release: v0.2.0"
   git push
   git tag v0.2.0
   git push origin v0.2.0
   ```

6. **Watch the run.** `release.yml` re-runs lint, typecheck, build, test and
   `verify:pack` before it publishes anything, then publishes with provenance
   and drafts the GitHub Release.

7. **Tell the docs queue.** Comment the release tag on every open docs issue
   covering a change that shipped:

   ```sh
   gh issue comment <n> --repo signalxjs/signalxjs.github.io \
     --body "Released in actors v0.2.0."
   ```

   A docs issue with no release comment means *merged but not released — do not
   document yet*. The comment is the docs agent's signal that the change is
   live.

## If a publish half-fails

`publish.js` publishes in dependency order and skips anything already on the
registry, so it is safe to re-run: fix the cause and re-run the workflow on the
same tag. Do not retag — a moved tag and a published version that no longer
matches it is worse than the original failure.

Never `npm unpublish`. Publish a patch that supersedes the bad version instead;
unpublishing breaks lockfiles for anyone who already installed it.
