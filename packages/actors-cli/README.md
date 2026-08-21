# @sigx/actors-cli

A [`sigx` CLI](https://sigx.dev/cli/) **plugin** for observing
[`@sigx/actors`](https://sigx.dev/actors): hosts, actors, latency, errors and
cluster topology, from the terminal.

```sh
pnpm add -D @sigx/actors-cli
```

```sh
sigx actors                                     # the dashboard (five tabs)
sigx actors top --url http://host:7311          # …against a running host
sigx actors stats --json                        # one snapshot, for piping
sigx actors health --url http://host:7311       # exit code by readiness
```

Two ways to reach a host: **embedded** (loads your app module in-process — note
it *starts a real host*, so keep it off production) and **HTTP** (`--url`,
polling a running host's `ops()` endpoint, no user code loaded). Only embedded
mode needs a project that depends on `@sigx/actors` — with `--url`, any
directory with this plugin installed will do: an ops box, a control plane, a CI
probe.

The `sigx` binary discovers plugins from the dependencies of the project you run
it in, so run it where the host lives — in a monorepo,
`pnpm --filter my-app exec sigx actors top`.

Requires **Node ^20.19.0 || >=22.12.0**. `@sigx/cli` (≥ 0.10) is a peer
dependency; `@sigx/actors` is an optional one.

Building another front end? Use
**[`@sigx/actors-monitor`](https://www.npmjs.com/package/@sigx/actors-monitor)**
— the renderer-free data layer this plugin draws, browser-safe and with no
CLI peer. (`@sigx/actors-cli/source` still re-exports it, but it also carries
`embeddedSource`, which starts a real host.)

## Documentation

**https://sigx.dev/actors/packages/actors-cli/overview/**

The ops endpoint it polls: https://sigx.dev/actors/docs/ops-endpoint/

Source, examples and architecture notes:
https://github.com/signalxjs/actors

MIT © Andreas Ekdahl
