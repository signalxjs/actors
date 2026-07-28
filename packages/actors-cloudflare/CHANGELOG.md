# Changelog

## [Unreleased]

### Added

- **Initial release** (#9): Cloudflare Durable Objects as the backend for
  `@sigx/actors`, on a **one-DO-per-actor** model.

  `durableObjectStorage(state.storage)` implements `ActorStorage`. DO
  storage is strongly consistent and single-threaded per object, so the
  runtime's etag compare-and-set holds without a transaction.

  `durableObjectReminders({ storage, alarms })` implements `ActorReminders`
  over the DO alarm. The default `shardedReminders()` splits one table into
  fixed hash shards and polls it, because a silo hosts many actors and has
  to find whose reminder is due; a DO hosts exactly one, so there is
  nothing to search and nothing to poll. It also removes the cadence floor
  — `shardedReminders` can only promise "at or after `nextDue`, checked
  every `reminderTickMs`", whereas an alarm fires at the due time. Call
  `onAlarm()` from the object's `alarm()` handler.

  No membership, directory, or authenticated internal mount: Cloudflare
  already guarantees a single instance of a DO globally and serializes its
  requests, which is the guarantee `@sigx/actors/cluster` exists to rebuild
  elsewhere.
