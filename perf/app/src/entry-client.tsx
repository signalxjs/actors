/**
 * The browser entry. `actor()` here resolves the build-swapped client ref,
 * so every actor call goes over the wire — and `useActorState` finds the
 * value SSR already put in the page under the same canonical key, so the
 * first paint costs no request at all.
 */
import { defineApp } from 'sigx';
import { hydrate } from '@sigx/server-renderer/client';
import { serverPlugin } from '@sigx/server/plugin';
import { actorsPlugin } from '@sigx/actors/app';
import { Room } from './Room';
import { roomFromPath } from './room-path';

defineApp(Room({ room: roomFromPath(location.pathname) }))
    .use(serverPlugin())
    // The transport is installed on live clients only; the endpoint the
    // build baked into each actor ref is the default target, so there is
    // nothing to configure for a same-origin app.
    .use(actorsPlugin())
    // `hydrate` matches the platform MountFn signature, so it drops in as
    // the explicit mount function — adopt the server's markup instead of
    // rendering over it.
    .mount(document.getElementById('app')!, hydrate);
