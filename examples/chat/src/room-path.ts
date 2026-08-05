/**
 * Room-from-URL: `/r/<name>` picks the room, anything else is #general.
 * Shared by the SSR entry (from the request URL) and the browser entry
 * (from `location.pathname`), so both render the same actor.
 */
const ROOM = /^\/r\/([\w-]{1,32})\/?$/;

export function roomFromPath(pathname: string): string {
    const match = ROOM.exec(pathname);
    return match ? match[1]! : 'general';
}
