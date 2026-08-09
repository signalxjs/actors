# @sigx/actors-cloudflare

Cloudflare **Durable Objects** as the backend for
[`@sigx/actors`](https://sigx.dev/actors) — one Durable Object per actor, so a
whole actor app runs on Workers.

- **`createHostDurableObject()`** — the object; hosts exactly the actor its id
  names.
- **`createWorkerHandler()`** — the Worker; hosts nothing, routes everything.
- **`durableObjectStorage()`** / **`durableObjectReminders()`** — the two seams
  the platform leaves to you, over DO storage and alarms.
- **`durableObjectPlacement()`** / **`durableObjects()`** — ref → object id.
- **`workerSocket()`** — the client WebSocket, terminated in the Worker; pairs
  with `socketTransport()` from
  [`@sigx/actors-ws`](https://sigx.dev/actors). It does **not** release
  `keptAlive` for departed live consumers (see
  [#47](https://github.com/signalxjs/actors/issues/47)).
- **`createHostDurableObject({ socket })`** — the client WebSocket terminated
  **in the object** (one socket per actor, hibernation-ready), which is the
  mode that DOES release a departed consumer's `keptAlive`. Forwarded by
  `createWorkerHandler({ socket: { terminate: 'object' } })`.

The package is small because Cloudflare already guarantees a single global
instance per object and serializes requests to it — that *is* the virtual-actor
contract, so there is no membership, no directory and no HMAC to rebuild.

> **Eviction is not deactivation.** The platform destroys the isolate, host and
> activation together, so **`onDeactivate` never runs** and `sweepIntervalMs`
> is 0. An actor that flushes in `onDeactivate` must `ctx.save()` inside the
> turn instead. Nothing already saved is lost; nothing unsaved survives.

```sh
pnpm add @sigx/actors-cloudflare
```

`@sigx/actors` is a peer dependency. Requires the `nodejs_compat` flag.

## Documentation

**https://sigx.dev/actors/packages/actors-cloudflare/overview/**

Deployment guide: https://sigx.dev/actors/docs/cloudflare-workers/

Runnable example (`examples/cf-workers`), source and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
