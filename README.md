# SignalX Actors

> Orleans-style **virtual actors** for [SignalX](https://github.com/signalxjs):
> addressable, single-threaded, persistent server objects riding sigx's
> serverFn wire protocol. Lazy activation, turn-based concurrency, pluggable
> storage with optimistic concurrency, timers & durable reminders, NDJSON
> streams, and a Vite plugin that swaps actor modules for typed client stubs.

```ts
export const CartActor = defineActor({
    type: 'Cart',
    use: [requireUser],
    state: () => ({ items: [] as Item[] }),
    methods: (ctx) => ({
        async addItem(item: Item) {
            ctx.state.items.push(item); // single-threaded — no races
            await ctx.save();           // persisted via ActorStorage
            return ctx.state.items.length;
        }
    })
});

// browser, serverFn, or SSR — the same expression:
await actor(CartActor, cartId).addItem(item);
```

## Packages

| Package | Description |
|---|---|
| [`@sigx/actors`](packages/actors) | The actor runtime, wire layer, and Vite plugin (entries: `.`, `./silo`, `./server`, `./node`, `./client`, `./cluster`, `./vite`) |
| [`@sigx/actors-redis`](packages/actors-redis) | Redis cluster providers — membership and the actor directory |
| [`@sigx/actors-cloudflare`](packages/actors-cloudflare) | Cloudflare Durable Objects backend — one DO per actor (storage + alarm reminders) |

Full documentation lives in the [package README](packages/actors/README.md).
Two runnable demos:

[`examples/chat`](examples/chat) — actors inside a **real SignalX app**: SSR
with `useActorState`, guards that run on both transports, a serverFn beside
the actor endpoint, and hydration with no refetch.

```sh
pnpm install && pnpm build
pnpm --filter chat-example dev                                    # dev silo through Vite
pnpm --filter chat-example build && pnpm --filter chat-example start
```

[`examples/counter`](examples/counter) — the same runtime with **no
framework at all**: plain DOM, plus a 3-silo cluster demo.

```sh
pnpm --filter counter-example dev       # single-silo dev server
pnpm --filter counter-example cluster   # 3-silo cluster demo over real HTTP
```

## Development

```sh
pnpm install
pnpm build       # build the package (dev + prod dists + d.ts)
pnpm test        # vitest (unit + wire integration + type contracts)
pnpm typecheck   # tsgo
pnpm lint        # oxlint
pnpm size        # size-limit bundle gates
```

This repo follows the sigx standard engineering setup
([`signalxjs/repo-template`](https://github.com/signalxjs/repo-template)):
issue → worktree (`pnpm wt new <N-slug>`) → PR → Copilot review →
squash-merge, with `main` protected. Agents: read [`AGENTS.md`](AGENTS.md).
Contributors: see [`CONTRIBUTING.md`](CONTRIBUTING.md).

## Status

v0.1 — single-node silo. The dispatch/placement seams are designed for
distributed backends (Cloudflare Durable Objects, clustering) to slot in
without public API changes; see "Design notes" in the package README.

## License

MIT © Andreas Ekdahl
