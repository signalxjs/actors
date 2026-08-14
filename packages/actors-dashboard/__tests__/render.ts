/**
 * Mount a panel into a real (happy-dom) document and read it back.
 *
 * `@sigx/runtime-dom/platform` is imported here rather than in each test:
 * it is what registers the default mount, and without it `app.mount(el)`
 * has nothing to render with. The published package deliberately does NOT
 * import it (see `src/mount.ts`) — the platform belongs to the app that
 * mounts the dashboard, so the tests play that app's part.
 */
import '@sigx/runtime-dom/platform';
import { defineApp, type JSXElement } from '@sigx/runtime-core';

export interface Mounted {
    el: HTMLElement;
    /** All visible text, whitespace-collapsed — what these tests assert on. */
    text(): string;
    /** `querySelectorAll`, as an array. */
    all(selector: string): Element[];
    one(selector: string): Element | null;
    unmount(): void;
}

/**
 * Render a vnode and hand back the container.
 *
 * Assertions go on the TEXT, not the styling: a colour here is a CSS custom
 * property resolved at paint time, and pinning one would fail the first time
 * somebody retunes the palette without changing a single fact on screen.
 */
export function mount(node: JSXElement): Mounted {
    const el = document.createElement('div');
    document.body.appendChild(el);
    const app = defineApp(node);
    app.mount(el);
    return {
        el,
        text: () => (el.textContent ?? '').replace(/\s+/g, ' ').trim(),
        all: (selector) => [...el.querySelectorAll(selector)],
        one: (selector) => el.querySelector(selector),
        unmount: () => {
            app.unmount();
            el.remove();
        }
    };
}
