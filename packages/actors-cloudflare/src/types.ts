/**
 * The slices of the Durable Objects API this package uses.
 *
 * Structural and hand-written, for the same reason `DurableStorage` is: the
 * package stays loadable and testable without a Workers runtime, and a test
 * can stand in a twenty-line fake instead of booting workerd. The real
 * `DurableObjectNamespace`/`DurableObjectState` satisfy these — a
 * compile-time conformance check against `@cloudflare/workers-types` lands
 * with the host class.
 *
 * Narrow on purpose. Every member here is one this package actually calls.
 */

/** A Durable Object id. `name` is populated only for `idFromName` ids. */
export interface DurableObjectIdLike {
    toString(): string;
    /**
     * The name this id was derived from, when it came from `idFromName`.
     *
     * Load-bearing: it is how a running object recognises its OWN actor
     * without reading storage, which is what makes the check available on a
     * cold `alarm()` before anything has been activated. `undefined` for an
     * id parsed from a string.
     */
    readonly name?: string;
}

/** A handle to one Durable Object. Only `fetch` is used. */
export interface DurableObjectStubLike {
    fetch(input: string | Request, init?: RequestInit): Promise<Response>;
}

export interface DurableObjectNamespaceLike {
    /** SHA-256 over the name — the same name always yields the same object. */
    idFromName(name: string): DurableObjectIdLike;
    get(id: DurableObjectIdLike, options?: { locationHint?: string }): DurableObjectStubLike;
    /**
     * A namespace scoped to a data-residency jurisdiction. Ids derived from
     * it DIFFER from the unscoped ones, so it is part of an actor's
     * identity, not a routing hint.
     */
    jurisdiction?(jurisdiction: string): DurableObjectNamespaceLike;
}
