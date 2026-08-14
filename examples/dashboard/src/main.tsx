/** @jsxImportSource sigx */
/**
 * The browser half — and it is this short on purpose.
 *
 * There is no `secret` here, and there is no host origin here. The browser
 * knows one thing: a path on its own server. Everything that makes the call
 * privileged happens on the other side of `/ops`, in `src/ops-proxy.ts`.
 *
 * `sigx` as the JSX source rather than `@sigx/runtime-core`: this is an app,
 * not a published library, so the umbrella (which registers the DOM platform
 * on its first line) is exactly right. `@sigx/actors-dashboard` itself must
 * not import it — see that package's tsconfig for why.
 */
import { defineApp } from 'sigx';
import { ActorsDashboard } from '@sigx/actors-dashboard';
import { httpSource } from '@sigx/actors-monitor';
import { OPS_MOUNT } from './config.public';

defineApp(
    ActorsDashboard({
        // Same-origin, no secret. The whole lesson of this example is that
        // these two facts are the same fact.
        source: httpSource({ url: location.origin, base: OPS_MOUNT }),
        intervalMs: 1000
    })
).mount(document.getElementById('app')!);
