# Conformance suites

A seam with several implementations gets **one shared suite**, not N copies of
the assertions.

Adding a provider or a transport means writing a *harness*, not a test matrix.

## Where they live, and why you cannot import them

| Subpath | Suite | Covers |
|---|---|---|
| `@sigx/actors/cluster/testing` | `transportConformance` | `HostTransport` implementations |
| `@sigx/actors/testing` | `bootstrapConformance` | provider schema bootstrap (`ensure…Schema`) |
| `@sigx/actors/testing` | `socketTransportConformance` | client `ActorTransport` implementations (#99) |
| `@sigx/actors/testing` | `storageConformance` | `ActorStorage` implementations and decorators (#65) |

All are **workspace-only**: wired by the tsconfig and vitest path aliases, and
deliberately **absent from `package.json` exports**. They cannot be imported
from outside this repo until someone decides to promote them, which is a
deliberate one-way door — publishing a test suite means supporting its shape.

Everything else in `packages/actors/package.json` `exports` is public API.
These are the exception, and `@sigx/actors/cluster/frames` and
`@sigx/actors/socket-wire` are the mirror image: published *precisely* so
out-of-repo transports can build on them.

`socketTransportConformance` follows `transportConformance`'s incumbent rule
one seam up: it runs against `fetchTransport()` — which shipped first, and
whose behaviour *is* the client contract — as well as `socketTransport()`.
The incumbent legitimately skips exactly the live case (it has no `live()`);
a skip is a reported outcome, never a silent pass.

## The storage suite

`storageConformance` is what "this is a storage the host can run on" means,
as fourteen cases over the four methods of `ActorStorage`: the load-miss
shape (`null`, never `undefined`), create/round-trip, the etag chain and
its staleness, create-over-existing and update-of-missing as conflicts,
`clear` as compare-and-delete with `null` asserting absence, no resurrection
of a cleared record by a stale writer, a refused write leaving the record
byte-identical (the precondition the host's corrupt-state handling rests
on), loaded records being the caller's to mutate (#25), non-object state
(arrays, scalars, `null`) staying distinct from absent, and keys being
opaque — NUL, separators, whitespace and escape-lookalikes are all distinct
records. The last three cases pin the optional `saveText` path (#238):
`saveText(json)` is `save(JSON.parse(json))`, it honours the same CAS and
brand, and the two paths share one etag chain.

Three things it deliberately does NOT assert, and why:

- **Concurrent writers.** Whether two racing saves produce exactly one
  winner is a property of the *backend's* atomicity — a Lua script, a
  commit-time conflict plus retry, a Durable Object's per-object
  serialisation. Each provider pins that with its own mechanism in its own
  test file; the suite asserts only what a single caller observes.
- **The save-side ownership rule.** `save` takes the tree at the call and
  the *caller* must not touch it afterwards — an obligation on the host,
  not an observable of the store. The load side is asserted.
- **The host's corrupt-state handling** — that is `runtime.test.ts`.

The incumbent is `memoryStorage`: its behaviour is the contract, and that it
passes is what proves the suite describes the seam rather than a newcomer's
habits. `fileStorage`, `durableObjectStorage` (over a Map), and — env-gated
like everything else against a live server — `pgStorage`, `redisStorage`
and `surrealStorage` all run the same list.

**Skips are for the text path only, and a harness can forbid them.**
`saveText` is optional on the seam, so a storage without it reports a skip
on the three text cases — `memoryStorage` wants the tree, and that is a
legitimate answer. But absence is also exactly what a *decorator* produces
when it returns a fixed three-method literal (the decorator rule on
`ActorStorage`): the host quietly falls back to the two-walk save path and
nothing says so. A harness sets `saveText: true` to declare the storage
implements it, and the text cases then FAIL when it is missing rather than
skip. The in-package run drives `metrics()` over a text-capable adapter
with that flag set, so a decorator dropping the member is a red case, not a
green skip.

The sabotage table in `packages/actors/__tests__/storage-conformance.test.ts`
is rule 2 made permanent: fourteen deliberately broken adapters — a miss
that loads as `undefined`, a save that ignores the etag, an unbranded
conflict, an upsert, a load that hands out the stored tree by reference, a
key-trimming layer, a non-injective NUL escape, a three-method decorator
over a text adapter, a `saveText` with its own etag chain — each named
against the case that must catch it, and the test asserts that case goes
red with a `[storage conformance]` message. A case added without an entry
there has not been shown to fail.

## The two rules

**1. Assert the outcome, never the mechanism.**

Postgres serialises its bootstrap with a `pg_advisory_xact_lock`. SurrealDB has
no lock primitive at all and converges by jittered retry. A case that pinned
either mechanism would be false for the other, so `bootstrapConformance`
asserts only what a caller can observe: a bootstrap leaves storage usable,
bootstrapping twice is a no-op, and N concurrent bootstraps from independent
connections all converge.

The same rule is why `transportConformance` was written against
`httpTransport()` *before* any connection-oriented transport existed — it
describes the contract rather than one implementation's habits. Two of its
cases are link-hygiene cases HTTP skips because it holds no connections;
`@sigx/actors-tcp` runs them all. (The retired host-to-host WebSocket
transport passed the full suite too — #151.)

Heterogeneous registration (#212) is part of the contract the suite pins:
`ConformanceClusterOptions.actorsFor` lets a case register different actors
per host, and the registration-aware case dispatches a type from a host that
does not register it, asserting it lands on one that does. Honoring
`actorsFor` is optional for a harness — the case verifies the topology took
effect via `descriptor().types` and reports a SKIP when it did not, the
`dropMembership` pattern: skipped is an outcome, a false green is not.

**2. A case that cannot fail is decoration.**

Prove a new case goes **red** against the unfixed code before trusting it. This
is the same discipline as the repo's test-first rule for bug fixes, and it
matters more here: a conformance case that silently passes everywhere gives
every future provider false confidence.

## Adding a provider

1. Write the harness the suite asks for — it supplies the wiring, the suite
   supplies the cases.
2. Run `bootstrapConformance` if the provider has a schema bootstrap. Concurrent
   boots from independent connections must converge; that is issues #76 and #78
   as a runnable assertion rather than a comment.
3. Run `storageConformance` if it implements storage. `storage()`/`stop()`
   are the shared intersection with the bootstrap suite and `bootstrap?()` is
   optional in both, so the harness you wrote for step 2 is the one this
   step wants. Each harness must hand out a storage over an EMPTY namespace
   (a fresh schema, key prefix, directory or Map) and `stop()` drops it —
   cases never clean up after themselves. Declare `saveText: true` if the
   adapter implements the text path, so a dropped member fails instead of
   skipping.
4. Gate the live-server tests on an env var (`REDIS_URL`, `PG_URL`,
   `SURREAL_URL`, `KUBECONFIG`) so the rest of the matrix skips cleanly, and
   add the CI job that provides it.

## Adding a transport

Run `transportConformance`. If a case does not apply to your transport, that is
a conversation about the contract — not a reason to skip it locally.

Both new-transport packages also inherit the frame codec from
`@sigx/actors/cluster/frames` rather than copying it; see
[wire-and-frames.md](wire-and-frames.md) for the behaviours the suite pins
(cancellation as a frame, backpressure at the generator, no retry of in-flight
calls on a dropped connection).
