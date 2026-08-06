# Architecture notes

Internal notes on how `@sigx/actors` is put together — for people changing it.

**This is not the manual.** How to *use* the runtime is documented at
**https://sigx.dev/actors**, and these files link there rather than restating
it. What lives here is the reasoning a user does not need and a maintainer
cannot work without: what each seam promises, what breaks if you widen it, and
which invariants are load-bearing.

These notes move with the code. If you change a seam, change the note in the
same PR.

## The shape, in one pass

A **call** enters through a mount, is resolved to a **dispatcher** by the
**placement**, and runs as a **turn** inside an **activation** that owns
**state** persisted through **storage**.

```
        client                     host process
  ┌──────────────┐        ┌──────────────────────────────────┐
  │ actor(T, k)  │        │  mount  ──►  placement           │
  │  .method()   │──wire──►  (server/  │     │               │
  │              │        │   node)    │     ├─► local dispatcher
  │ ActorTransport        │            │     │      │        │
  │ ActorRouter  │        │            │     │      ▼        │
  └──────────────┘        │            │     │  activation   │
                          │            │     │   ├─ turn queue
                          │            │     │   ├─ ctx.state │
                          │            │     │   └─ ActorStorage
                          │            │     │               │
                          │            │     └─► HostTransport ──► peer host
                          │  ActorScheduler drives everything background
                          └──────────────────────────────────┘
```

Every arrow that crosses a box is a seam, and every seam is listed in
[`seams.md`](seams.md).

## The files

| File | Answers |
|---|---|
| [`seams.md`](seams.md) | What are the extension points, what does each promise, and which are public? |
| [`runtime-internals.md`](runtime-internals.md) | What is a turn, what runs outside one, how is a deadlock detected? |
| [`wire-and-frames.md`](wire-and-frames.md) | What is on the wire, which mounts exist, what is reserved? |
| [`clustering.md`](clustering.md) | How do hosts find each other and agree on who owns an actor? |
| [`conformance-suites.md`](conformance-suites.md) | How is a new provider or transport proven correct? |

## Two rules that shape most of the code

**Core stays WinterCG-clean and zero-dependency.** `@sigx/actors` and
`@sigx/actors/cluster` must run on Cloudflare Workers, so anything needing
`node:*` or a third-party client is a separate package
(`@sigx/actors-tcp`, `@sigx/actors-redis`, …). This is why there are so many
seams: each one is a place a platform-specific package plugs in without core
learning about it.

**Never depend on `sigx` from shipped code.** The umbrella package's first line
imports the DOM runtime, which is wrong for a terminal app, a Lynx app or a
headless host. `./app` imports `@sigx/runtime-core` instead. The build keeps
`sigx` external as a guard.
