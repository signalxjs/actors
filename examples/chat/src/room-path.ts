/**
 * Room-from-URL: `/r/<name>` picks the room, anything else is #general.
 * Shared by the SSR entry (from the request URL) and the browser entry
 * (from location.pathname), so both render the same grain.
 *
 * One hardcoded room means one hot grain on one silo — routing the room
 * through the URL is what makes the cluster part of the demo visible.
 */
const ROOM = /^\/r\/([\w-]{1,32})\/?$/;

export function roomFromPath(pathname: string): string {
    const match = ROOM.exec(pathname);
    return match ? match[1]! : 'general';
}
