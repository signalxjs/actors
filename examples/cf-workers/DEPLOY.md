# Deploying this example

Every step below was run against a real Cloudflare account before it was
written down, and the outputs are the ones that came back. Where something
broke, the failure is included — those are the parts worth reading.

## Prerequisites

- **Node 22+.** `wrangler@4` declares `engines.node >= 22`.
- `npx wrangler login`, or `CLOUDFLARE_API_TOKEN` + `CLOUDFLARE_ACCOUNT_ID` in
  the environment. Check with `npx wrangler whoami`.
- **Plan:** SQLite-backed Durable Objects are available on the Workers **Free**
  plan, which is what `new_sqlite_classes` gets you. Sustained alarm testing
  and repeated deploys will want Workers Paid.
- `pnpm install && pnpm build` at the repo root first — the example bundles the
  workspace packages.

## (a) The one irreversible decision

`wrangler.jsonc` uses:

```jsonc
"migrations": [{ "tag": "v1", "new_sqlite_classes": ["ActorHost"] }]
```

**`new_classes` is a one-way door.** It creates the legacy key-value backed
storage, which cannot be migrated to SQLite in place — the class is stuck with
it — and carries a much smaller per-value limit. Get this right before the
first deploy, because the first deploy is when it is decided.

## (b) Local first

```sh
pnpm --filter cf-workers-example dev
# second terminal
TARGET_URL=http://127.0.0.1:8787 pnpm --filter cf-workers-example verify
```

```
  ok    exact counts under sequential increments
  ok    no lost updates under 20 concurrent increments
  ok    K keys are K independent actors
  ok    a cross-actor call lands in the callee
  ok    a watch stream delivers a frame
  ok    a reminder fires from a real alarm
{"mode":"verify","checks":6,"failures":[]}
```

## (c) Deploy

```sh
pnpm --filter cf-workers-example deploy
```

Read the warnings. This one is **not** cosmetic:

```
▲ [WARNING] The package "node:async_hooks" wasn't found on the file system but
  is built into node.
  Your Worker may throw errors at runtime unless you enable the
  "nodejs_compat" compatibility flag.
   - @sigx/server/dist/server-UDKqOesV.js
```

Without `compatibility_flags: ["nodejs_compat"]` the Worker deploys fine, the
edge answers, and **every actor call fails**:

```json
{"error":{"message":"[sigx actors] host do:Counter/a (https://sigx.invalid) is unreachable; the call may be retried.","status":503,"data":{"kind":"unreachable"}}}
```

The message points at the stub fetch, but the cause is upstream: the Durable
Object cannot boot, so the fetch has nothing to reach. `wrangler dev` and the
`vitest-pool-workers` suite both enable the flag implicitly, so this only ever
shows up on a real deployment. It is already set in this example's config.

## (d) Verify the deployment

```sh
TARGET_URL=https://<name>.<subdomain>.workers.dev \
  pnpm --filter cf-workers-example verify
```

All six checks pass against production. The one that matters most is the last:
it is the only check a plain request cannot stand in for.

## (e) Reminders fire with nobody watching

The definitive test, and worth doing by hand once:

```sh
U=https://<name>.<subdomain>.workers.dev
K="quiet-$(date +%s)"
curl -s -X POST "$U/_sigx/actor/Ticker%23once" \
  -H 'content-type: application/json' -d "{\"args\":[\"$K\",45000]}"
# → {"data":true}

# now touch NOTHING for a minute.

curl -s -X POST "$U/_sigx/actor/Ticker%23ticks" \
  -H 'content-type: application/json' -d "{\"args\":[\"$K\"]}"
# → {"data":1}
```

No request reached that object between arming and checking. The platform woke
it from its alarm, the host booted cold, and the reminder table's persisted
owner ref is what told it whom to deliver to. That last part is why the owner
rides alongside the entries in storage rather than in memory.

`pnpm --filter cf-workers-example tail` shows the `alarm()` invocation with no
inbound request beside it.

## (f) Per-key isolation, from the outside

```sh
for k in a b c; do
  curl -s -X POST "$U/_sigx/actor/Counter%23increment" \
    -H 'content-type: application/json' -d "{\"args\":[\"$k\",1]}"; echo
done
# → {"data":1} three times: three keys, three objects, three counters.
```

## (g) State survives a redeploy

Increment a key, `pnpm --filter cf-workers-example deploy` again, read it back.
The value is unchanged: state lives in Durable Object storage, not in the
isolate. This is the most reassuring thing to see once.

## (h) Load — and what it will not tell you

```sh
TARGET_URL=$U MODE=load HOT=1 CONCURRENCY=32 DURATION_S=10 \
  pnpm --filter cf-workers-example verify
TARGET_URL=$U MODE=load KEYS=32 CONCURRENCY=32 DURATION_S=10 \
  pnpm --filter cf-workers-example verify
```

Measured from one laptop against a real deployment:

| concurrency | 1 hot key | 32 keys |
|---|---|---|
| 8 | 79 ops/s (p50 89ms) | 47 ops/s (p50 103ms) |
| 32 | 264 ops/s (p50 108ms) | 237 ops/s (p50 123ms) |

The hot key is *ahead*, which is the opposite of the "one object is a
bottleneck" story. Both runs are dominated by round-trip latency to the edge
and by cold-start cost — 32 keys means creating 32 objects — and neither gets
near a single object's turn-serialization ceiling. That ceiling is real, but demonstrating
it needs a generator near the edge and far more concurrency than this.

Use `MODE=load` to characterise **your** access pattern, and be suspicious of
any number produced from a laptop.

## (i) Teardown

```sh
pnpm --filter cf-workers-example exec wrangler delete --force
```

```
Successfully deleted sigx-actors-example
```

**Deleting the Worker destroys its Durable Object namespaces and all actor
state in them.** There is no undo. A crashed run can leave a live Worker
behind, so the name is stable and predictable on purpose — check with
`npx wrangler deployments list` if unsure.

To remove one class but keep the Worker, use a `deleted_classes` migration
rather than editing the binding out. Likewise, renaming an exported class on
a deployed Worker needs a `renamed_classes` migration (`{ "tag": "v2",
"renamed_classes": [{ "from": "OldName", "to": "NewName" }] }`) — editing
`class_name` in place strands the old objects and their state.

## What this example cannot show you

- **Colo placement.** A Durable Object is created near its first caller; one
  laptop cannot demonstrate that.
- **Real eviction timing.** Cloudflare decides when an idle object is evicted,
  and it is not a documented interval. `evictDurableObject()` in the workerd
  suite forces it instead.
- **Alarm retry with real backoff.** The platform re-invokes a throwing
  `alarm()`; the runtime persists the advanced due time before delivering, so a
  retry skips rather than double-fires — proven by construction and in the
  workerd suite, not here.
- **`sigx actors top`.** The dashboard polls `ops()` on a long-lived host. A
  Worker has none, and a Durable Object is evicted between calls, so the
  dashboard does not apply to this backend.
