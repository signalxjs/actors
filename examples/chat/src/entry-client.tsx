/**
 * The browser entry. `actor()` here resolves the build-swapped client ref,
 * so every actor call goes over the wire — and `useActorState` finds the
 * value SSR already put in the page under the same canonical key, so the
 * first paint costs no request at all.
 *
 * The wire is the client SOCKET (#99): one WebSocket for the page's whole
 * actor traffic, dialed lazily on the first call. The three `{ live: true }`
 * reads each cost one ~40-byte `{i,sub}` frame on it — no held-open POST,
 * and a subscription-set change no longer reopens anything. `useActorState`
 * itself needed zero changes for this; the transport is the entire swap.
 * The cookie that signed you in rides the upgrade, so the socket carries
 * the same identity as every fetch. serverFns (`postMessage`, `me`) are a
 * different surface and stay on HTTP.
 */
import { defineApp } from 'sigx';
import { hydrate } from '@sigx/server-renderer/client';
import { serverPlugin } from '@sigx/server/plugin';
import { actorsPlugin } from '@sigx/actors/app';
import { socketTransport } from '@sigx/actors-ws/client';
import { Room } from './Room';
import { roomFromPath } from './room-path';

const socketUrl =
    (location.protocol === 'https:' ? 'wss://' : 'ws://') + location.host + '/_sigx/socket';

defineApp(Room({ room: roomFromPath(location.pathname) }))
    .use(serverPlugin())
    .use(actorsPlugin({ transport: socketTransport({ url: socketUrl }) }))
    // `hydrate` matches the platform MountFn signature, so it drops in as
    // the mount function — adopt the server's markup rather than render
    // over it.
    .mount(document.getElementById('app')!, hydrate);
