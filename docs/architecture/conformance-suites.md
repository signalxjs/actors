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
describes the contract rather than one implementation's habits. Of its 18
cases, two are link-hygiene cases HTTP skips because it holds no connections;
`@sigx/actors-tcp` passes all 18. (The retired host-to-host WebSocket
transport passed all 18 too — #151.)

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
3. Run the `ActorStorage` suite if it implements storage. `storage()`/`stop()`
   are the shared intersection with the bootstrap suite and `bootstrap?()` is
   optional, so the two compose.
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
