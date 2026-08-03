/**
 * The app, as a FACTORY — and that is the whole lesson of this example.
 *
 * `examples/counter` builds its app at module scope, which is right for a
 * process that owns one host. On Workers it is wrong: storage and reminders
 * come from a Durable Object's own `ctx.storage`, so they exist per OBJECT,
 * not per module. A module-scope app would bind whichever object happened to
 * construct it first and quietly serve every other one from those seams.
 *
 * So the host hands the seams in, and this only adds what is
 * runtime-independent — plugins, defaults, context extensions.
 */
import { defineActorApp, metrics, type ActorAppOptions } from '@sigx/actors/host';

export const createApp = (base: ActorAppOptions) => defineActorApp(base).use(metrics());

/**
 * A type-only binding so actor modules can import an app-bound `defineActor`
 * without importing a running app. Never started — the host starts the real
 * one, per object.
 */
export const { defineActor } = createApp({});
