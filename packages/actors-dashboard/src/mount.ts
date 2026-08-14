/**
 * `mountActorsDashboard` — the escape hatch for a host page that is not a
 * sigx app.
 *
 * A sigx app should render `<ActorsDashboard />` directly: its platform is
 * already registered, and mounting a second app inside one is not what
 * anybody wants. This exists for the other case — an admin portal built on
 * something else, or a plain page — where there is no app at all.
 *
 * `@sigx/runtime-dom/platform` is imported DYNAMICALLY, and that is the whole
 * reason this lives in its own file. It is the module that registers the
 * default mount and evaluates the render machinery, so a static import would
 * put a DOM renderer in the module graph of a package whose main entry has to
 * import cleanly in bare Node (`scripts/verify-pack.js`) and on a server
 * during SSR. Mounting is inherently a browser act, so paying for it there is
 * honest.
 */
import { defineApp } from '@sigx/runtime-core';
import { ActorsDashboard, type ActorsDashboardProps } from './dashboard';

/**
 * Render the dashboard into `element`.
 *
 * Resolves to the unmount function. **Call it** when the page tears the
 * dashboard down: the poll loop is stopped by the component's own unmount
 * hook, and dropping the element without unmounting leaves it polling the
 * cluster for the lifetime of the tab.
 */
export async function mountActorsDashboard(
    element: Element,
    options: ActorsDashboardProps
): Promise<() => void> {
    await import('@sigx/runtime-dom/platform');
    const app = defineApp(ActorsDashboard(options));
    app.mount(element);
    return () => app.unmount();
}
