# SignalX actors — shared agent guide

> ⚠️ **BRANCH FIRST — never work on `main`.** Before touching ANY file, create a
> worktree (`pnpm wt new <N-short-slug>`) and do everything from
> `<repo>/branches/<N-short-slug>`. This applies to every change, however small —
> editing or committing in the primary checkout (`<repo>/main`) causes conflicts
> for parallel sessions. Check yourself before every commit:
> `git branch --show-current` must print your worktree's branch name — if it
> prints `main` or nothing (detached HEAD), stop.
> Already edited files in `main` by mistake? Move the work, don't commit it:
> `git stash -u` → `pnpm wt new <N-short-slug>` →
> `cd <repo>/branches/<N-short-slug>` → `git stash pop`.

Canonical guidance for **any** AI agent working in this repo (Claude Code, GitHub
Copilot CLI, work agents, …). Tool-specific notes live in `CLAUDE.md`; it defers
here for everything shared — when it conflicts with this file, the tool-specific
file wins for that tool only.

This is the sigx standard agent setup. The same pattern (this file +
`scripts/worktree.mjs` + a thin tool-specific file) is used across sigx repos —
it originates in [`signalxjs/repo-template`](https://github.com/signalxjs/repo-template).
See "Adopting this setup in another sigx repo" at the bottom.

SignalX Actors is the home of `@sigx/actors` — virtual actors for
SignalX: addressable, single-threaded, persistent server objects riding
the serverFn wire protocol. A pnpm workspace (ESM, `"type": "module"`) with
one published package under `packages/`, runnable demos under `examples/`,
and the performance/deployment rig under `perf/` — which is deliberately
NOT under `examples/`, because it is measured, not read.
Tech stack: TypeScript (strict), Vite, Vitest, oxlint.
Published to npm under the `@sigx` scope.

## Development workflow (issue → PR → Copilot review → merge)

**This is mandatory for EVERY agent-driven change — including one-line fixes.
Never commit straight to `main`.** Repo: `signalxjs/actors`, base branch `main`.
(Human contributors follow `CONTRIBUTING.md`, where an issue is optional; for
agents the issue-first flow below is required.)

1. **Issue first.** If no GitHub issue already tracks the work, create one *before*
   writing code and put the plan in it:
   ```sh
   gh issue create --title "<concise title>" --body "<what & why, plus the plan/checklist>"
   ```
   If you worked in plan mode, the approved plan **is** the issue body. Note the
   number it returns (`#N`).

2. **Worktree, always.** Never work on `main`. Use the worktree flow (below):
   `pnpm wt new <N-short-slug>` gives an isolated checkout on branch
   `<N-short-slug>`. Don't substitute `git switch -c` in the primary checkout —
   it occupies `<repo>/main`, which parallel sessions share.

3. **Implement & verify.** For a **bug fix, write a failing unit test that
   reproduces the bug *first*** (red), then make the fix so that test passes
   (green) — see "Test-first bug fixes" under Conventions. Either way, prove the
   change: `pnpm typecheck` (always, for any `.ts`) plus the relevant `pnpm test`
   / `pnpm build`. Stage specific files (`git add <path>`), never `git add -A`.
   No co-author trailers.

4. **Open a PR with Copilot as the reviewer.** Reference the issue so it auto-closes
   on merge:
   ```sh
   gh pr create --base main --title "<title>" \
     --body "Closes #N. <short summary of the change>" --reviewer @copilot
   ```
   The PR description becomes the squash commit **body** verbatim, and the PR
   title (with ` (#<pr>)` appended) becomes its subject — see step 6. Write the
   description as the commit body you want on `main`.
   (On an already-open PR: `gh pr edit <pr> --add-reviewer @copilot`.) The bot
   `copilot-pull-request-reviewer` posts its review within a minute or two. If your
   `gh` is too old to resolve `@copilot` (error: `'@copilot' not found`), request it
   via the API instead — don't skip it:
   ```sh
   gh api --method POST repos/signalxjs/actors/pulls/<pr>/requested_reviewers \
     -f 'reviewers[]=copilot-pull-request-reviewer[bot]'
   ```
   (The reviewer-request API takes the `[bot]`-suffixed slug; the review author
   login in `.reviews[].author.login` appears *without* the suffix.)

5. **Wait for Copilot's review, then fix.** Do not merge before it has reviewed. Poll
   until a review by the bot appears, then read it:
   ```sh
   gh pr view <pr> --json reviews -q '.reviews[].author.login'   # wait for "copilot-pull-request-reviewer"
   gh pr view <pr> --json reviews,comments
   ```
   Address every actionable comment with follow-up commits and push. If the review
   doesn't re-trigger on its own, re-request it: `gh pr edit <pr> --add-reviewer @copilot`.
   Repeat until Copilot has no remaining actionable feedback.

   **Then resolve the threads.** Where the repo's ruleset sets
   `required_review_thread_resolution` (check with
   `gh api repos/signalxjs/actors/rules/branches/main`), a PR carrying an
   unresolved **inline** comment cannot merge however green it is — with a
   merge queue it silently never enqueues, and `gh pr checks` shows nothing
   wrong. Pushing the fix does not resolve a thread, and neither does replying
   at PR level. There is no `gh pr` porcelain — reply on each thread and
   resolve it over GraphQL:
   ```sh
   # list the open threads
   gh api graphql -f query='query { repository(owner:"signalxjs", name:"actors") {
     pullRequest(number:<pr>) { reviewThreads(first:100) { nodes {
       id isResolved comments(first:1){nodes{body}} } } } } }' \
     -q '.data.repository.pullRequest.reviewThreads.nodes[]
         | select(.isResolved==false) | "\(.id) \(.comments.nodes[0].body[0:60])"'

   # reply (say which commit fixed it), then resolve — pass the body as a
   # GraphQL variable, not string-interpolated: quotes and backslashes in a
   # review reply otherwise break the query
   gh api graphql -f query='mutation($t:ID!,$b:String!){
     addPullRequestReviewThreadReply(input:{pullRequestReviewThreadId:$t, body:$b}){ comment { id } } }' \
     -f t="<thread-id>" -f b="Fixed in <sha>. <what changed>"
   gh api graphql -f query='mutation($t:ID!){
     resolveReviewThread(input:{threadId:$t}){ thread { isResolved } } }' -f t="<thread-id>"
   ```

6. **Merge it yourself.** Once Copilot's feedback is resolved, CI is green, and —
   for user-facing changes — the docs issue is filed on the docs repo and linked
   from the PR (see "Documentation"), merge (squash — repo rules block merge
   commits) and clean up:
   ```sh
   pr=123                                     # your PR number (digits only)
   gh pr checks "$pr"                         # must be all green first
   gh pr merge "$pr" --squash --delete-branch \
     --subject "$(gh pr view "$pr" --json title -q .title) (#$pr)" \
     --body "$(gh pr view "$pr" --json body -q .body)"
   ```
   Pass `--subject`/`--body` explicitly, exactly as above — GitHub appends
   `Co-authored-by:` trailers to every message it generates itself (in **all**
   squash-message modes, even PR_TITLE/PR_BODY) whenever a branch-commit author
   differs from the merging account; an explicit message is used verbatim, so
   no trailers. If you used a worktree, remove it afterward: `pnpm wt rm <name>`.

## Build, Test, Lint

```bash
pnpm install
pnpm build       # build all packages
pnpm test        # vitest run (unit tests across packages)
pnpm test:workers # the Cloudflare suite on REAL workerd (own config + CI job)
pnpm test <path>                   # single test file/dir (substring match)
pnpm test -t "name of test"        # single test by name (vitest -t)
                                   # NB: no `--` — vitest discards operands
                                   # after it, so `pnpm test -- x` silently
                                   # runs the WHOLE suite. pnpm forwards
                                   # args natively; the `--` is npm-only.
pnpm test:watch
pnpm test:coverage
pnpm typecheck   # tsgo (a fast TS compiler), config: tsconfig.json
pnpm lint        # oxlint over the packages' src
pnpm lint:fix
pnpm size        # size-limit bundle-size check (.size-limit.json)
pnpm verify:refs # resolve every `#N` in the guarded paths against the repo

pnpm bench       # build, then run every benchmark scenario
pnpm bench:run <filter>   # skip the build; filter by scenario name substring
pnpm bench:baseline       # record this machine's reference (gitignored)
pnpm bench:compare        # run again and diff against that reference
pnpm bench:diff --before=a.json --after=b.json   # diff two saved result files
pnpm bench:ab --base-dir=<checkout> --head-dir=<checkout> --out=rounds.json
                          # interleaved counterbalanced A/B of two BUILT checkouts
pnpm bench:ab:report --rounds-file=rounds.json   # verdict table from those rounds
pnpm bench:profile <s>    # same, under --cpu-prof (writes benchmarks/profiles/)
pnpm bench:ws             # sockets/*: connection scale on a live cluster (opt-in)
pnpm bench:ws:baseline    # record this deployment SHAPE's socket baseline
pnpm bench:ws:compare     # run again and diff against it
pnpm bench:tier2          # Tier 2: real sockets, one process per host (opt-in)
pnpm bench:threads        # compute/: worker_threads vs the event loop (opt-in, needs cores)
```

Benchmarks measure the built `dist/*.prod.js` via `--conditions=production`,
so `pnpm build` must be current — `pnpm bench` does it for you. They need
Node >= 22.18 (native `.ts` type stripping). Before trusting a comparison,
read the "Trusting the numbers" section of `benchmarks/README.md` — a
contended machine produces false regressions, and the suite says so when it
detects one.

**In CI they run, and the split between what gates and what does not is the
whole design.** The `Bench` workflow (`.github/workflows/bench.yml`) fires on a
PR touching `packages/**`, `benchmarks/**` or the lockfile and measures the base
ref against the head ref in **interleaved, counterbalanced rounds** on the
dedicated bench VM (#98, ported from core#639): even rounds base-first, odd
rounds head-first, so machine drift cancels in the paired deltas instead of
accruing to whichever side ran second — sequentially, that order was worth a
measured ~+2% to the head. The PR comment is a *verdict* per row, not a bare
delta. `pnpm bench:compare`'s local baseline stays per-machine and gitignored
for the same reason it always was. To judge any two refs on the VM without
opening a PR: `gh workflow run bench.yml -f base=<ref> -f head=<ref>
-f rounds=7` (`-f enforce=true` makes a unanimous timing regression fail the
run — the deliberate proof mode, never the PR path).

- **Timings never gate on the PR path.** A row is only *called*
  (`improved`/`regressed`) when every round agrees in sign AND the median delta
  clears both a 3% floor and the row's own run-to-run spread; a side that
  swings more than 10% reads `noisy` and claims nothing. Even a called
  regression is advisory on a PR — reproduce it (more rounds via dispatch, or
  locally on a quiet machine) before acting on it.
- **`exact` metrics gate, at zero tolerance, and FAIL the check.** Invariants
  rather than measurements — `directory_ops` per activation, `microtask_turns`
  per dispatch, `notifications` per join, the `prefer-local` locality
  guarantees, the topics fan-out counts (`deliveries_per_publish`,
  `remote_dispatches_per_publish`) — so a shared runner judges them as well as
  a quiet desk. In that
  same identical-commit run, every one came back bit-identical while 215 other
  metrics drifted. Adding `exact: true` to a metric that is not deterministic
  *by construction* (anything on `randomPlacementPolicy()`, on
  `consistentHashPolicy()` — whose host ids are minted per run — on a clock, or
  on the heap) breaks the gate for everyone, so read `Metric.exact` in
  `benchmarks/src/types.ts` first.
- **The merge queue measures only the gating scenarios.** `merge_group` takes
  no `paths` filter, so it fires on every queued merge; re-running the whole
  suite there would tax each one to re-check timings that cannot fail.
  `BENCH_GATE_SCENARIOS` in the workflow is that list, and
  `benchmarks/__tests__` asserts it stays in step with the `exact` flags — a
  scenario filter is a substring match, so a rename would otherwise shrink the
  gate silently.
- A scenario that *throws* fails the step either way.

See "What a shared runner actually does" in `benchmarks/README.md` for the
measurements all of this is calibrated against.

Most scenarios are **Tier 1**: one process, zero sockets, measuring
algorithmic shape. **Tier 2** (`cluster2/*`, `pnpm bench:tier2`) forks a
process per host and uses real loopback TCP; it is opt-in via `BENCH_TIER2=1`
and never runs as part of `pnpm bench`. Inside it, counts (sockets, bytes)
gate while timings are `informational` — see `benchmarks/BASELINES.md`, whose
tier legend exists so a modelled figure is never quoted as a measured one.

To run an example/app: `pnpm --filter <package-name> dev`.

## Packages

- `packages/actors` → `@sigx/actors` — the virtual-actor runtime. Eleven
  runtime entries (plus types-only `./vite-client`): `.` (defineActor + isomorphic `actor()`,
  `defineWorker` — stateless multi-activation pure-compute pools: always
  local, no directory claim, up to `maxLocal` concurrent members per
  key — plus
  topics — `topic()`/`publishTopic()`, the `subscriptions:` table and
  `ctx.publish` for actor-to-actor pub/sub), `./host` (defineActorApp
  + the plugin model, createHost, runtime, memoryStorage), `./server`
  (WinterCG wire endpoint, `onMiss` proxy/redirect/auto for actors another
  host owns, plus `actorRouteToken()` — read-only; the endpoint never
  validates the routing token), `./node`
  (createAppHandler + connect adapter, fileStorage, signal handlers),
  `./client` (build-swap target, configureActors, the `ActorTransport`
  seam + `fetchTransport`, the `route` option minting the per-actor
  routing token, and the `ActorRouter` seam — `routedTransport`,
  `learningRouter`, `routedFetchTransport` — opt-in by import so it
  tree-shakes), `./job`
  (`defineJob` — durable long-running operations: state machine, progress,
  checkpoint/pause/resume, `watch()`; convention over `defineActor` + the
  `tasks:` primitive), `./app` (`actorsPlugin()` — the sigx app
  integration; it imports `@sigx/runtime-core`, NEVER the `sigx`
  umbrella — see below), `./cluster` (the
  `cluster()` plugin, clusterPlacement (incl. `locate()` and
  `publicAddress`), host-to-host endpoint, cluster provider seams,
  memoryClusterHub), `./cluster/frames` (the framed host-to-host wire —
  published precisely so out-of-repo transports can build on it;
  `@sigx/actors-tcp` re-exports from it),
  `./socket-wire` (the CLIENT socket vocabulary of #99 —
  `SocketRequest`/`SocketReply` plus the codec and pollution-safe parse;
  published for out-of-repo adapters, and deliberately NOT the cluster
  frames: no field for a principal, no envelope, no inbound calls), `./vite`
  (`sigxActors()` plugin). Two further subpaths are **workspace-only** — wired
  by tsconfig/vitest aliases and deliberately absent from `package.json`
  exports: `./cluster/testing` (`transportConformance`) and `./testing`
  (`bootstrapConformance`, `socketTransportConformance` — the client
  `ActorTransport` suite of #99, run by `fetchTransport` and
  `@sigx/actors-ws` — and the home of the `ActorStorage` suite of #65).
- `packages/actors-redis` → `@sigx/actors-redis` — Redis (≥7) providers:
  `redisCluster` (membership and the actor directory) for
  `@sigx/actors/cluster`, and `redisStorage` (etag-CAS `ActorStorage` —
  the cluster-safe persistence option). ioredis ≥5 as a peer dependency;
  provider tests are env-gated on `REDIS_URL`.
- `packages/actors-pg` → `@sigx/actors-pg` — Postgres (≥13) providers, for
  the team whose one durable store is SQL: `pgStorage` (etag-CAS
  `ActorStorage`, single-statement CAS, state as JSON in a `text` column —
  NOT `jsonb`, which rejects the NUL an actor key may carry), `pgMembership`
  (TTL heartbeat judged on the DATABASE clock, LISTEN/NOTIFY push with
  poll fallback, signature-based change detection so silent deaths
  converge without a version bump), `pgDirectory` (claim/CAD/`evictHost`),
  `pgReminders` (durable reminders on a due-time-indexed table, one
  SKIP LOCKED claim statement per tick — the reminder-scan answer from
  #16 for pg deployments), `pgCluster` bundling membership + directory,
  and `pgSchemaSql()`/`ensurePgSchema()` —
  DDL is explicit, the providers never issue it; `ensurePgSchema` takes a
  `pg_advisory_xact_lock` as the FIRST statement of the same values-free
  simple query (that string is ONE implicit transaction, which is what makes
  the xact lock cover the DDL and release before the pooled client returns), so
  every replica may bootstrap at boot. `pg` ≥8 as a peer
  dependency; provider tests are env-gated on `PG_URL` (a dedicated CI
  job provides a postgres service).
- `packages/actors-surreal` → `@sigx/actors-surreal` — SurrealDB (≥3.0,
  3.2.4+ recommended) providers: `surrealStorage` (etag-CAS `ActorStorage`
  on a COMPOSITE record id `{prefix}state:[type, key]` — the primary index,
  so no load or save ever scans; state held as a JSON STRING so top-level
  arrays, scalars, `null` and NUL round-trip exactly), `surrealMembership`
  (TTL heartbeat on the DATABASE clock, live-query push with poll fallback,
  signature-based change detection), `surrealDirectory` (claim/CAD/
  `evictHost`), `surrealReminders` (due-time-indexed table),
  `surrealCluster`, `surrealSchemaSql()`/`ensureSurrealSchema()` and
  `surrealRetryable`. `surrealdb` ≥2.0.8 as a peer dependency; tests
  env-gated on `SURREAL_URL` (its CI job starts the container with
  `docker run`, NOT `services:` — the image is `ENTRYPOINT ["/surreal"]`
  with no CMD, so a service entry cannot pass the `start` subcommand).
  **Four v3 rules this package is built around, all verified against a live
  3.2.4 server — do not "simplify" past them**: (1) `UPSERT … WHERE` does
  NOT gate its create path, so it resurrects a record a concurrent writer
  deleted — state CAS uses `UPDATE … WHERE` plus an explicit create; (2)
  there is no `SELECT … FOR UPDATE`, `SKIP LOCKED` or advisory lock, so the
  commit-time write–write conflict is the only mutual exclusion and client
  RETRY IS PART OF THE CONTRACT (`surrealRetryable`; a shared connection
  must install it, and the SDK ships retry disabled with a predicate that
  never matches) — which is also why reminders partition by `ownsShard`,
  the opposite of `pgReminders`, and why `ensureSurrealSchema` converges by
  its OWN bounded retry rather than the connection's (#76: the DDL races
  too, and the predicate cannot be widened without governing the caller's
  queries); (3) `UPDATE`/`DELETE` ignore indexes, so
  predicate-driven writes go through a `SELECT` subquery; (4) reading an
  undefined table ERRORS (2.x returned `[]`), so the DDL step is mandatory —
  which is also what makes the bootstrap's "are the tables there?" probe a
  plain `SELECT` that parses nothing.
  Record-scoped live queries also fail to listen on 3.2.4 while
  table-scoped ones work, which is why membership watches the version
  table.
- `packages/actors-k8s` → `@sigx/actors-k8s` — Kubernetes membership
  provider for `@sigx/actors/cluster`: `k8sMembership()`, host liveness via
  a coordination.k8s.io Lease per host (renewed on the heartbeat cadence)
  and a label-selected Lease watch feeding the membership view. Node-only
  (`node:https`/`node:fs`), zero runtime deps — talks to the API server
  with fetch, no client lib. The actor directory stays store-backed
  (compose with `redisDirectory`). Tested against a fake API server;
  real-cluster suite env-gated on `KUBECONFIG`.
- `packages/actors-tcp` → `@sigx/actors-tcp` — a framed TCP
  transport for `@sigx/actors/cluster`: `tcpTransport()`, one multiplexed
  connection per peer instead of HTTP's one per in-flight request. Node-only
  (`node:net`), zero runtime deps. Justified by socket count, not latency —
  see `benchmarks/BASELINES.md`. Runs the shared transport conformance suite.
  The one blessed socket transport for hosts: a host-to-host WebSocket
  transport existed and was retired (#151) — an edge runtime should be a
  *client* of the deployment (#99), not a cluster peer, and a WS host
  transport can be rebuilt out-of-repo on `./cluster/frames` if one-port
  L7-ingress traversal ever matters.
- `packages/actors-ws` → `@sigx/actors-ws` — the CLIENT-facing WebSocket
  transport (#99): `socketTransport()` on `./client`, WinterCG-clean,
  speaking `@sigx/actors/socket-wire` to `createActorSocketSession` on
  `@sigx/actors/server`. The connection seam is a link, not a URL
  (`connect(handlers): SocketLink`; `url` is `WebSocket` sugar), it is
  host-affine (per-call `endpoint` ignored with a `__DEV__` warning —
  every call re-dispatches through placement), and in-flight calls FAIL
  un-retried when the socket drops. Zero runtime deps; tested against the
  real session over an in-memory link.
- `packages/actors-cloudflare` → `@sigx/actors-cloudflare` — Durable
  Objects as the backend, one DO per actor. A whole app runs on Workers:
  `createHostDurableObject()` (the object) + `createWorkerHandler()` (the
  edge), over `durableObjectStorage`, `durableObjectReminders` (alarms) and
  `durableObjectPlacement`/`durableObjects()`. Needs no membership,
  directory or HMAC — Cloudflare already guarantees single-instance — but it
  DOES use the internal mount: the Worker→object hop is `httpTransport()`
  with its `fetch` swapped for a stub call, so envelope, NDJSON, deadlines
  and branded errors are the runtime's own. The placement runs on BOTH sides
  with an `isSelf` predicate; giving the object's own host the plain local
  host instead activates a callee INSIDE the caller's object and corrupts
  state (break `isSelf` and the test suite OOMs — the object fetches itself
  forever). **Eviction is not deactivation**: the platform destroys the
  isolate, host and activation together, so `onDeactivate` never runs and
  `sweepIntervalMs` is 0. **A DO stub is never cached** — it is an I/O object
  bound to the request that made it, so reusing one across requests makes
  every later call "unreachable". Two suites: fakes under `__tests__` (fast,
  in `pnpm test`) and REAL workerd under `__tests__/workers` (`pnpm
  test:workers`, its own config and CI job — wrangler needs Node >= 22 and
  the main matrix includes 20).
- `packages/actors-cli` → `@sigx/actors-cli` — a `@sigx/cli` PLUGIN (the
  `@sigx/lynx-cli` shape, not its own binary) that observes hosts:
  `sigx actors stats` and `sigx actors health`, over an embedded source
  (loads the project's app module in-process) or an HTTP one (polls a
  running host's `ops()` endpoint). `@sigx/actors-cli/source` is the
  renderer-free data layer, deliberately reusable by a future web
  dashboard. `@sigx/cli` and `@sigx/terminal` are NOT core packages, so
  they take literal version specs rather than `catalog:`.
- `packages/actors-otel` → `@sigx/actors-otel` — observability exporters:
  `prometheusOps()` + the pure `renderPrometheus()` on the
  OTel-free `./prometheus` entry (text exposition from the metrics digest;
  real histograms from the raw log-linear buckets via
  `bucketUpperBoundsUs()`; the `ops()` bearer posture verbatim, secret
  mandatory outside dev), `otelTraces()` (a CLIENT span per dispatch from
  `useDispatch`, a SERVER span per turn from the turn observer's `call`
  parameter, joined across hosts by the runtime-propagated
  `ActorCallContext.traceparent`; one branch per dispatch when no provider
  is registered), and `otelMetricsBridge()` (observable instruments over
  `metrics().digest()`, one batch callback per collection; distributions
  stay with Prometheus — the OTel metrics API cannot ingest pre-bucketed
  histograms). Labels are type/method only, never keys.
  `@opentelemetry/api` ≥1.9 as an OPTIONAL peer (literal spec, not
  `catalog:`); tests run in-process on the OTel SDK's in-memory
  exporters — no env gate, no extra CI job.
- `examples/chat` — actors in a REAL sigx app, and the composition proof:
  `sigx()` + `sigxServer()` + `sigxActors()` in one Vite build, SSR-seeded
  `useActorState`, one auth policy enforced on BOTH transports (the wire
  call and the in-process SSR dispatch), a serverFn calling an actor
  in-process, hydration with no refetch,
  `useActorState(…, { live: true })` — one multiplexed `$live` connection
  for the page, so every open tab stays current — a topics-fed cross-room
  activity feed (rooms publish `room-activity`, a singleton `ActivityFeed`
  projects it, and `ctx.principal` survives the publish hop so entries are
  attributed), `migrateState` on `Room`, and HMAC-signed HttpOnly sessions.
  Dev and prod start the same app module. Single-node, `fileStorage`, one
  listener on **5290** — clustering is a README section pointing at
  `examples/counter`, which shows it with no infrastructure. Deliberately
  NOT production-shaped: the deployment-shaped version of this app is
  `perf/app`, and conflating the two is what made this example unreadable
  before. Not published.
- `examples/counter` — the same runtime with NO framework (plain DOM):
  dev host, client swap, streams, file persistence, and a runnable 3-host
  cluster demo (`pnpm --filter counter-example cluster`), and the
  durable-job demo (`pnpm --filter counter-example job`): a checkpointing
  `defineJob` whose owning host is killed mid-run and which resumes from
  its last checkpoint on a survivor. Also the two runtime primitives no
  other example runs (`pnpm --filter counter-example runtime-demo`, NOT
  `runtime` — that name collides with a pnpm builtin): `ctx.tasks` detached
  work mutating through `ctx.turn` while reads keep answering, and a
  durable `ctx.reminders` entry that outlives the host that armed it and
  re-activates the actor on a new one. Not published.
  `pnpm --filter counter-example cluster:serve` keeps that cluster UP
  under steady traffic, with `metrics()` and `ops()` mounted — the
  target `sigx actors top` is demonstrated against.
- `examples/cf-workers` — the Cloudflare deployment example: one Durable
  Object per actor, a Worker that hosts nothing and routes everything, and
  `verify.mjs` (six correctness checks, one JSON summary line, including a
  reminder firing from a REAL alarm). The point of it is the app **factory**:
  `examples/counter` builds its app at module scope, which on Workers binds
  whichever object constructed it first. Three things it documents because
  they all bite: `new_sqlite_classes` is a one-way door, `__DEV__` must be
  `define`d or the host throws on the first request, and the public mount
  needs an explicit `origin` policy. `MODE=load` exists but its local numbers
  describe `wrangler dev`, not Cloudflare — see the README. Not published.
- `perf/aks` → `sigx-perf-aks` — the production-shaped Kubernetes
  deployment test, and the reason `perf/` exists as a tree of its own: it
  is 4k lines of rig around a 300-line app, which under `examples/` read
  as the thing to copy.
  N identical host pods (env-driven `server.mjs`, `cluster()` with
  `redisCluster` or `k8sMembership` per values toggle, `redisStorage`
  CAS persistence), an in-cluster closed-loop load generator, a Helm
  chart, and `deploy/testenv.mjs` — the whole Azure environment as one
  command per verb (up / status / test / baseline / bench / load /
  ws-up / ws-load / ws-bench / migrate-check / down). The WebSocket half (#172) is
  its own axis and measures different nouns: `ENABLE_SOCKET` mounts
  `attachActorSocket` plus a `socketStats()` ops section on the host,
  `Fanout` is the actor a subscriber watches, and `ws-loadgen.mjs`
  reports CONNECTIONS HELD and MESSAGES DELIVERED rather than ops/s
  (`ws-dev.mjs` runs the same path single-host with no Redis). `ws-load`
  is the hand-run; `ws-bench` is the RECORDED one — it computes the
  actors release's own `INFRA_SHAPE` (socket caps included, so a run under
  a different `SOCKET_MAX_SUBSCRIPTIONS` is refused rather than compared)
  and hands off to the `sockets/*` scenarios in
  `benchmarks/src/scenarios/sockets.ts` (#184). The orchestration itself
  lives in `deploy/ws-load.mjs` so both callers share it.
  Two facts bound every number it produces: client subscriptions cannot
  set `throttleMs`, so all of them run at the fixed 50 ms watch throttle;
  and a live read that consults `ctx.principal` gets one watch loop per
  identity (#121) while a cross-host watch coalesces per principal unless
  the method declares `principalIndependent` (#138) — which `Fanout`
  deliberately does NOT, because measuring the undeclared cost is the
  experiment — so anonymous and signed-in fan-out are
  different measurements and are never averaged. Identity (RG, cluster, ACR, region, DNS, load-VM
  names) is REQUIRED env with no defaults — the code is public, the
  estate is not; in CI the values come from Actions secrets (see
  `deploy/RUNBOOK.md` §1c). `test` runs `__tests__/infra.test.ts`
  (env-gated on `INFRA_URL`, chaos behind `INFRA_CHAOS=1`) and then the
  Tier-3 bench compare; the Tier-3 scenarios live in
  `benchmarks/src/scenarios/infra.ts` (`BENCH_INFRA=1`, `pnpm
  bench:infra`) and drive load from a same-region VM via
  `deploy/edge-ladder.mjs`. The charts (this one and
  `perf/app/deploy/`) are validated by `.github/workflows/charts.yml`
  against a kind cluster with the ingress-nginx admission webhook — the
  check `helm lint` cannot be. Not published.
- `perf/app` → `sigx-perf-app` — the Tier-3 SYSTEM UNDER TEST: the app the
  harness deploys and measures. It was `examples/chat` until the two were
  split, which is why it carries a dual listener, a Helm chart, a
  Dockerfile, `Digest`/`DigestActor`/`Stall` load fixtures the UI never
  touches, and ~20 env knobs (`PLACEMENT`, `REBALANCE*`, `MAX_ACTIVATIONS`,
  `SWEEP_INTERVAL_MS`, `DIGEST_*`, `MIGRATE_PERSIST`) that `INFRA_SHAPE` is
  computed from — `testenv.mjs` reads them back off the live Deployment so a
  comparison across shapes is refused rather than quietly wrong.
  **It is pinned.** Changing an actor body, a knob default or the chart
  invalidates the recorded baselines in `benchmarks/BASELINES.md`; that is a
  deliberate act with numbers attached, not a tidy-up. Do not "improve" it
  to look like the example — the readable version is `examples/chat`, and
  keeping them one file is what made that example unreadable. Replacing it
  with a purpose-built headless workload is #85. Not published.
- `benchmarks` → `actors-benchmarks` — local performance baselines:
  closed-loop throughput and latency percentiles against the BUILT prod
  dist, per-actor heap footprint, leak detection, and the CPU/allocation
  profiling recipes. Run by hand (`pnpm bench`), and by the `Bench`
  workflow, which A/Bs a PR's base ref against its head ref on one runner
  through `benchmarks/src/compare-files.ts` and comments the delta. **A
  timing there never gates; an `exact` metric does and FAILS the check** —
  see "Build, Test, Lint" above and the `Metric.exact` contract in
  `benchmarks/src/types.ts`. `benchmarks/__tests__` covers that REPORT (the
  only part of the suite a human acts on without seeing the numbers), not
  the measurements.
  See `benchmarks/README.md`, and `benchmarks/BASELINES.md` for the
  reference figures. Not published.

Path aliases: `tsconfig.json` and `vitest.config.ts` map `@sigx/actors` and
its subpaths to `packages/actors/src`, so tests and typecheck run against
source, not dist. `benchmarks/src` is in the tsconfig `include` (so it IS
typechecked and linted, and `benchmarks/__tests__` is typechecked and run by
vitest) but resolves `@sigx/actors` from `dist/` at runtime
— deliberate: types come from source, measurements from the shipped build. The example resolves `@sigx/actors` from the built `dist/`
via the workspace link — run `pnpm build` before `pnpm --filter
counter-example dev`.

## Parallel work with git worktrees

To work two things at once — each with its own checkout and its own agent
session — use a worktree instead of switching branches in place:

```sh
pnpm wt new <name> [--from <branch>]   # worktree at <repo>/branches/<name>: own branch + deps installed
pnpm wt list                           # show all worktrees
pnpm wt rm <name> [--force]            # remove a worktree
```

Layout convention (all sigx repos): the primary checkout lives at `<repo>/main`
and every worktree at `<repo>/branches/<name>`. `pnpm wt new` creates the
checkout there on a new branch `<name>` and runs `pnpm install` (pnpm hardlinks
from the global store — fast). Launch a **separate agent session from the
worktree directory**; sessions stay independent per directory. Names: letters,
digits, `.`, `_`, `-` only.

## Documentation

Docs are part of the change, not a follow-up — in-repo docs ship in the same
PR, and the docs-site update is queued (as a docs-repo issue) before merge. Two
surfaces, two rules:

**In-repo docs — update in *this* PR when you touch the matching thing:**

| When you… | Update… |
|---|---|
| add / rename / remove a package | `AGENTS.md` "Packages" and the README package table — plus, **whichever of these the repo has**: `CONTRIBUTING.md` layout, the issue-template package dropdowns, `.size-limit.json`, and the `tsconfig` / `vitest` path aliases |
| change a build / test / lint script | `AGENTS.md` "Build, Test, Lint", `CONTRIBUTING.md` "Common tasks", `package.json` |
| change or add public API / behaviour | the package's `CHANGELOG.md` under `[Unreleased]` — **plus a docs-repo issue**, because the manual is the docs site (see "Package READMEs are pointers" below) |
| change a seam, an invariant, or how the runtime fits together | the matching file in [`docs/architecture/`](docs/architecture) |
| change the workflow / process itself | `AGENTS.md` here — and, since it is the shared standard, upstream the same change to [`signalxjs/repo-template`](https://github.com/signalxjs/repo-template) |
| add or change an example | the example's own `README.md` — and hold it to the convention below |

**Package READMEs are pointers, not manuals.** Every published package's
`README.md` is ~30 lines: thesis, install, peer-dependency and minimum-version
requirements, and links to <https://sigx.dev/actors>. Do not grow them back — the
package README was 2,632 lines of second copy of the manual, and it had drifted
into seven confirmed factual errors by the time it was cut (#113). Anything
worth writing goes to the docs site (via a docs-repo issue) if a *user* needs
it, or to [`docs/architecture/`](docs/architecture) if a *maintainer* does.

All links in a published README must be **absolute URLs** — npm does not
resolve relative paths, so `[…](../actors)` renders as a broken link on the
package page.

**Every `examples/*` has all four of these.** They exist because the examples
drifted badly enough to need a cleanup once: one had no README at all, another
had grown into a performance rig with a third of its source unreachable from
its own UI, and the best-written of the three was the one nobody was being
pointed at.

1. **A `README.md` on the `examples/cf-workers` skeleton** — a one-sentence
   thesis; a copy-pasteable quickstart **with its real output**; "what to look
   at when you open it", including explicit non-goals and cross-references to
   the other examples; one named lesson worth copying; "things that will bite
   you" as bolded failure modes; and a Files table covering every tracked file.
   Quote transcripts from an actual run — a paraphrased one goes stale silently.
2. **A `description` in `package.json`.**
3. **A `tsconfig.json` and a `typecheck` script**, chained into the root
   `typecheck`. Map `@sigx/actors*` through `paths` to `packages/*/src`, as
   cf-workers does: CI typechecks **before** it builds, so an example resolving
   the package's `exports` (which point at `dist/`) fails on a clean checkout.
   Ambient files from that source — `src/env.d.ts` for `__DEV__`,
   `src/vite/virtual.d.ts` for `virtual:sigx-actors` — need naming in `include`
   for the same reason.
4. **Its `src` in the root `lint`.** `.mjs` entries stay out, matching
   `verify.mjs`.

**An example is not a test rig.** Anything that exists to be *measured* —
load fixtures, perf knobs, deployment charts — belongs in `perf/`, even when
that means two versions of the same app. Keeping them as one file is what
made the chat example unreadable.

**The docs *site* is separate — don't edit it from here.** User-facing changes
(new or changed public API, features, packages) must end up documented on the
docs site [`signalxjs/signalxjs.github.io`](https://github.com/signalxjs/signalxjs.github.io),
but that work belongs to the **docs agent**, which works through the docs repo's
issue queue. Don't open docs-site PRs from source repos — your job is to feed
the queue, in two moments:

- **Before merging a PR with user-facing changes, file an issue on the docs
  repo** describing what changed and what the docs need to cover, and link it
  from the PR:
  ```sh
  gh issue create --repo signalxjs/signalxjs.github.io \
    --title "actors: <what changed>" \
    --body "Source: signalxjs/actors#<pr>. <What needs documenting, and where on the site.> Not yet released."
  ```
  A user-facing PR isn't mergeable until its docs issue exists (see step 6 of
  the workflow).
- **When you cut a release** (push a `vX.Y.Z` tag), comment the release tag on
  every open docs issue covering a change shipped in that release:
  ```sh
  gh issue comment <n> --repo signalxjs/signalxjs.github.io \
    --body "Released in actors vX.Y.Z."
  ```
  (Mention the published package version(s) too if they differ from the tag.)
  A docs issue without a release comment means *merged but not released — don't
  document yet*; the release comment is the docs agent's signal that the change
  is live and ready to document.

## Conventions & working principles

- **Never depend on `sigx` from shipped code.** The published packages must
  not pull a renderer. `sigx` is an umbrella whose first line is
  `import '@sigx/runtime-dom/platform'`, so depending on it drags the DOM
  runtime in behind it — wrong for a terminal app, a Lynx app, or a
  headless host, none of which have a DOM. `./app` needs framework
  primitives, so it imports **`@sigx/runtime-core`** (and
  `@sigx/runtime-core/internals`), which is what `sigx` re-exports anyway:
  the types are identical and consumers see no difference. The build keeps
  `sigx` external as a guard, so a reappearing import stays unbundled
  rather than silently shipping. Tests may use `sigx` — they are not
  shipped, and the ones that mount a real app *should*, because that is the
  integration they exist to prove.
- **Plan first for non-trivial work.** Both Claude Code and Copilot CLI have a built-in plan mode; use it and let the CLI manage the plan file.
- **Verify before declaring done.** Run typecheck/tests for code changes; show evidence the change works.
- **Test-first bug fixes.** Reproduce the bug with a *failing* unit test first (red), then make the fix so the test goes green — the failing test proves both that the bug exists and that the fix actually addresses it, and it stays behind as a regression test. Never fix a bug without a test that would have caught it. While you're in the area, if you find behaviour that should be covered but isn't, add the missing tests in the same PR.
- **A seam with several implementations gets ONE shared conformance suite, not N copies of the assertions.** They live in `@sigx/actors/cluster/testing` (`transportConformance`) and `@sigx/actors/testing` (`bootstrapConformance`) — framework-free case descriptors, parameterized by a harness the package supplies, run by every implementation. Two rules make them worth having: **assert the outcome, never the mechanism** (Postgres serialises its bootstrap with an advisory lock; SurrealDB has no lock primitive and converges by retry — a case that pinned either one would be false for the other), and **a case that cannot fail is decoration** — prove a new one goes red against the unfixed code before trusting it. Concretely: a provider package with a schema bootstrap runs `bootstrapConformance`, so concurrent boots from independent connections must all converge (#76, #78). Adding a provider means writing a harness, not a test matrix.
- **Minimal, surgical edits.** Don't refactor unrelated code. Don't add backward-compat shims for things that never shipped.
- **Cross-platform paths**: Contributors and CI can run on Windows, macOS or Linux (check this repo's CI matrix for what it actually covers) — use the path separator and shell syntax of the environment you're in, and prefer Node scripts over shell one-liners for anything committed to the repo.
- **Git hygiene**: Stage specific files (`git add <path>`), never `git add -A` / `git add .`. Run `pnpm typecheck` before any commit touching `.ts`. Do **not** add co-author trailers to commits (e.g. `Co-Authored-By: Claude …` / `Co-authored-by: Copilot …`).

## Adopting this setup in another sigx repo

This file, `scripts/worktree.mjs`, and `CLAUDE.md` are the portable sigx
standard, maintained in [`signalxjs/repo-template`](https://github.com/signalxjs/repo-template).
To adopt it in another repo:

1. Check the repo out using the standard layout: primary checkout at
   `<repo>/main`, worktrees under `<repo>/branches/`.
2. Copy `scripts/worktree.mjs` and `CLAUDE.md` verbatim; copy this `AGENTS.md` as a template.
3. Add `"wt": "node scripts/worktree.mjs"` to the repo's `package.json` scripts.
4. Adapt the repo-specific sections of `AGENTS.md`: the intro (what the repo is),
   "Build, Test, Lint", and "Packages". Replace every `actors` with the repo name.
5. Keep the workflow, worktree, and conventions sections as-is — they are the
   shared standard.
6. Lock down `main`: `node scripts/apply-branch-protection.mjs signalxjs/actors`.
