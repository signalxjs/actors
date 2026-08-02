/**
 * The entry — and on Cloudflare the Durable Object and the Worker are the
 * SAME bundle, which is why the actor registry is a plain import here and
 * needs no build-time indirection.
 *
 * Two exports, two jobs:
 *
 *  - `ActorHost` is the object. It hosts exactly the actor its id names.
 *  - `default` is the edge. It hosts nothing and routes everything.
 *
 * Both need the registry: the Worker to run guards, tell a stream method from
 * a unary one, and be the 404 authority; the object to actually activate.
 */
import { createHostDurableObject, createWorkerHandler } from '@sigx/actors-cloudflare';
import { createApp } from './actors.app';
import { Counter } from './counter.actor';
import { servePage } from './page';
import { Ticker } from './ticker.actor';

/**
 * Declared by hand rather than generated with `wrangler types`.
 *
 * One binding is not worth a generated file that has to be committed, kept in
 * step with `wrangler.jsonc`, and excluded from lint — and two sources of
 * truth for `Env` is worse than one short interface.
 */
export interface Env {
    ACTORS: DurableObjectNamespace;
}

const actors = [Counter, Ticker];

export class ActorHost extends createHostDurableObject<Env>({
    actors,
    namespace: (env) => env.ACTORS,
    // Receives the object's own storage/reminders/defaults and must pass them
    // on. Never receives `env`, so it cannot reach a namespace binding and
    // build a placement of its own.
    app: createApp
}) {}

export default createWorkerHandler<Env>({
    actors,
    namespace: (env) => env.ACTORS,
    app: createApp,
    fetch: {
        // Workers callers are not browsers posting a form, and the public
        // mount defaults to refusing a request with no Origin. Decide it
        // explicitly; a real app with a browser front-end should pass its
        // own origin list here instead.
        origin: false,
        // Anything that is not an actor call falls through here, so one
        // Worker serves the page and the actors.
        fallback: servePage
    }
});
