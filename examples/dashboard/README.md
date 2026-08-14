# dashboard example

The `@sigx/actors` web dashboard against a **real three-host cluster**, and
the same-origin proxy that keeps the ops bearer token off the client.

```sh
pnpm install && pnpm build          # the example resolves the workspace source

# terminal 1 — three hosts on 5391-5393, under steady traffic
pnpm --filter counter-example cluster:serve

# terminal 2
pnpm --filter dashboard-example dev
```

```
  VITE v8.1.5  ready in 585 ms

  ➜  Local:   http://localhost:5490/
  ➜  Network: use --host to expose
  ➜  ops proxy: /ops  →  http://127.0.0.1:5392  (bearer attached here, never in the browser)
```

Open <http://localhost:5490>. Five tabs, live at 1 Hz:

```
cluster · 2 host(s)
hosts 2   activations 34   queued 0   calls 2.06k  93 failed (5%)
calls/s 6/s   failures/s 0/s   queued 0   activations 34
latency p50 2.56ms p90 6.91ms p99 12.3ms
queue   p50 28µs   p90 160µs  p99 336µs
turn    p50 34µs   p90 88µs   p99 216µs
```

```
host          status   ready  up      acts  queue  view  transports
s.rw1fgvap    active   yes    2m51s   29    0      #5    http
s.zulfsnby    leaving  NO     2m51s   5     0      #5    http
```

That second host is the demo draining one on purpose: **live 200, ready 503**.
Drain it, do not restart it — and note the dashboard shows *both* hosts'
readiness, not just the one it polled.

## The lesson worth copying: the ops secret never reaches the browser

This is the whole reason the example exists, and it is not a style preference.
Two facts about `ops()` force it:

- **It sets no CORS headers.** A browser cannot call it cross-origin at all.
- **It refuses to construct without a bearer secret outside `__DEV__`**,
  because it reports your actor type names, traffic shape and cluster
  topology — and the activation list carries actor **keys**, which are user
  data.

Reach for the secret to get past the first and you have handed every visitor
the second. So the browser talks to *this app's* origin:

```ts
// src/main.tsx — no secret, no host address, just a path
httpSource({ url: location.origin, base: '/ops' })
```

and `src/ops-proxy.ts` is the other side: check the caller, then attach the
token. It is a plain Node handler used by **both** the Vite dev server
(`vite.config.ts` mounts it as middleware) and `server.mjs`, because an
example whose security boundary exists in two copies has already lost it.

The `isOperator` hook is where your real session check goes. It defaults to
"anyone" here because a demo has no login — and it is the only thing between
an anonymous visitor and your cluster topology, so it is not the line to
leave for later.

**And the split is structural, not a bundler optimisation.** The secret lives
in `src/config.server.ts`; `src/config.public.ts` holds the mount path and
nothing else; `main.tsx` imports only the second, so there is no module path
from browser code to the first. Tree-shaking *would* drop an unused
`OPS_SECRET` today — but a guarantee that rests on an optimiser staying clever
breaks quietly the first time somebody adds a side effect or logs the config
object. `__tests__/no-secret-in-browser.test.ts` walks the real import graph
and fails if it can reach the secret, `process.env`, or the host origin.

## What to look at when you open it

- **`src/main.tsx` is nine lines.** That is the point: `<ActorsDashboard>` is
  a drop-in, and every panel is also exported on its own (`HostsPanel`,
  `ClusterPanel`, …) taking `{ state }`, so a portal can embed one table
  instead of the shell.
- **Click a host row.** The drill-down is not just a different view — it
  changes what is *requested*. Watch the network tab: the poll gains
  `?detail=1&host=…`, because a detail poll makes that host walk its
  activation table and nobody should pay for a panel they are not looking at.
  Leave the tab and it stops immediately.
- **Retheme it without touching a rule.** Every colour and metric is a
  `--sigx-actors-*` custom property; set them on any ancestor. `styles={false}`
  plus the exported `actorsDashboardCss` covers a strict CSP.
- **Compare it with the terminal.** Same data layer, two renderers:
  ```sh
  pnpm --filter counter-example exec sigx actors top \
      --url http://127.0.0.1:5392 --secret demo-ops-secret
  ```
  The numbers agree because neither renderer derives them —
  `@sigx/actors-monitor` does, once. That is the claim
  `docs/architecture/monitoring.md` makes, and this is how you check it.

**Non-goals.** It is not a production admin portal: no login, no
multi-tenancy, no persistence. It is not a performance rig either — that is
`perf/`, deliberately a tree of its own. For actors inside a real sigx app,
read `examples/chat`; for the runtime with no framework at all, and for the
cluster this dashboard is pointed at, read `examples/counter`.

## Things that will bite you

- **Pointing at 5391 gives you a dead host.** `cluster:serve` *kills* the
  first host on its way past, to show the survivors re-forming and reclaiming
  its reminder shards. The default here is 5392 for that reason. Any one
  surviving host is enough — it fans out to the rest.
- **`ops()` is not mounted by default.** The cluster demo wires it by hand
  (`ops({ secret, cluster: (signal, query) => clusterStats(placement, { signal, ...query }) })`),
  because `ops()` lives in `/host` and `clusterStats` in `/cluster` — a
  single-node host must not pay for the cluster bundle to have an ops
  endpoint. Without that second argument, `/ops/cluster` answers 404 and the
  Cluster tab says "single-node".
- **A proxy that drops the query string breaks the drill-down silently.**
  `httpSource` appends `/cluster` and `?detail=1&host=…` itself. Forward the
  path and query verbatim, or every drill-down sits on "waiting for a detail
  poll…" forever with no error anywhere.
- **`startsWith('/ops')` is too loose.** It also matches `/opsummary`, and
  would forward your bearer token to a URL nobody meant to build. The handler
  checks for the exact mount or a `/` or `?` after it; `__tests__` pins that.
- **`server.mjs` needs Node >= 22.18** for the `.ts` imports (built-in type
  stripping), same as `examples/counter/server.mjs`. `pnpm dev` has no such
  requirement — Vite compiles them.

`pnpm start` serves the same thing from `dist/` on the same port, and prints
the same proxy line:

```
dashboard    http://localhost:5490
ops proxy    /ops  →  http://127.0.0.1:5392  (bearer attached here, never in the browser)
```

## Files

| File | What it is |
|---|---|
| `index.html` | The page. Its own chrome only; everything inside `#app` is the dashboard's. |
| `src/main.tsx` | The browser half — nine lines, no secret, no host address. |
| `src/ops-proxy.ts` | **The point of the example.** Same-origin route → `ops()`, bearer attached server-side. |
| `src/config.public.ts` | The one constant the browser may see: the mount path. Nothing else may go here. |
| `src/config.server.ts` | Port, host and **the secret**. Nothing in the browser's import graph may reach it. |
| `__tests__/no-secret-in-browser.test.ts` | Walks `main.tsx`'s import graph and fails if it can reach the secret, `process.env` or the host origin. |
| `src/static.ts` | Path containment for the static server (a raw `/../package.json` must not escape `dist/`). |
| `vite.config.ts` | Dev server on 5490, mounting the proxy as middleware. |
| `server.mjs` | Production entry: the built client plus the same proxy. |
| `__tests__/ops-proxy.test.ts` | The proxy's behaviour, pinned — it is a security boundary, not a demo. |
| `tsconfig.json` | Resolves `@sigx/actors*` from source, so a clean checkout typechecks before it builds. |
| `package.json` | Scripts and workspace deps. |
