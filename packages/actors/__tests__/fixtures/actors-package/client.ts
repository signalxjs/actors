/**
 * The BROWSER half: typed refs, no implementation.
 *
 * This is the same swap the Vite plugin performs for first-party
 * `*.actor.ts` modules, done statically in the package's exports map
 * instead — so it works for any bundler, not only Vite.
 *
 * The `import type` below is ERASED at runtime: the types come from the
 * real definitions so consumer code is fully typed, while the values are
 * refs. Nothing from `./server` is bundled.
 */
import { __actorRef } from '@sigx/actors/client';
import type { Greeter as GreeterDef, Presence as PresenceDef } from './server';

/** Stream names must match the definition — they drive wire routing. */
export const Greeter = __actorRef('acme/greeter', '/_sigx/actor', [
    'watch'
]) as typeof GreeterDef;

export const Presence = __actorRef('acme/presence', '/_sigx/actor') as typeof PresenceDef;
