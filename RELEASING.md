# Releasing

All twelve `@sigx/actors*` packages release together, on one version, from one
tag. Publishing is done by CI over npm **trusted publishing** (OIDC) — there is
no npm token in this repo, and no workflow references one.

## One-time setup

Trusted publishing is configured per package on npmjs.com, and it binds to an
exact `owner/repo` **and** workflow path. For each of the twelve packages, set:

- Repository: `signalxjs/actors`
- Workflow: `.github/workflows/release.yml`

A run from any other repository is rejected at token exchange, so this has to
be redone if the repo ever moves.

**A NEW package needs its own entry before its first CI publish.** Trusted
publishing is configured per package and cannot be set on a name that does not
exist on npm yet, so a package added to this repo is not covered by the others'
configuration. The bootstrap is: publish it once by hand (`pnpm publish:all`
skips everything already on the registry, so it publishes only the new one),
then add its trusted-publishing entry, after which every later release goes
through CI with provenance like the rest.

## Cutting a release

1. **Land everything first.** The tag is cut from `main`; nothing is published
   from a branch.

2. **Move the versions.**

   ```sh
   pnpm version:minor          # or version:patch / version:major
   pnpm version:set 0.2.0      # …or an exact version
   ```

   This rewrites `version` in all twelve manifests **and** the cross-package
   `peerDependencies` ranges that point at them. Both halves matter: the eleven
   sibling packages peer on `@sigx/actors`, and publishing them against a stale
   range means demanding a version that is no longer `latest`.

   It deliberately leaves `workspace:` specifiers alone — `@sigx/actors-cli`
   and `@sigx/actors-dashboard` take real (non-peer) dependencies on
   `@sigx/actors-monitor`, and `pnpm pack` rewrites those to a concrete range
   at publish time. Which is also why `publish.js` lists `actors-monitor`
   before both of them: with a real dependency the order is load-bearing, not
   cosmetic.

3. **Move the changelogs.** In each package that changed, rename the
   `[Unreleased]` heading to the new version with today's date, and open a
   fresh `[Unreleased]` above it.

4. **Verify locally.**

   ```sh
   pnpm typecheck && pnpm lint && pnpm test && pnpm build
   pnpm verify:pack            # packs all twelve, installs them, import-smokes each entry
   pnpm publish:dry
   ```

   `verify:pack` is the one that catches packaging mistakes nothing else sees —
   a missing `files` entry, a broken `exports` map, a stale `dist/`, or a
   tarball with no LICENSE in it.

5. **Land the bump through a PR, then tag the merge commit.** `main` is
   protected — a direct push is rejected by the branch ruleset, so the bump is
   a normal PR like any other change (#133).

   ```sh
   pnpm wt new <n>-release-0.2.0
   git commit -am "release: v0.2.0"
   gh pr create --base main --title "release: v0.2.0" --reviewer @copilot
   # …review, merge, then from an up-to-date main:
   git pull --ff-only origin main
   git tag v0.2.0
   git push origin v0.2.0
   ```

   Tag the **merged** commit, not the branch: the tag must match the manifests
   exactly, and the release workflow refuses to publish otherwise, because
   `publish.js` skips versions already on the registry and would otherwise
   produce a GitHub Release with no packages behind it.

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
