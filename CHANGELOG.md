# Changelog

## 0.2.1 - 2026-11-27

- Add opt-in `mcp.activity` collection for Signals contract 2.8.0, independently of REST collection. PostgreSQL admission precedes tool dispatch; terminal outcomes, deadlines, immutable delivery parts and receipts determine finite coverage intervals.
- Require explicit dispatcher-inventory confirmation before complete coverage. Unknown outcomes, missing evidence and erasure remain partial; complete measures admitted attempts, not service availability or all offered traffic.
- Add bounded object-reference sets, dispatch-order prefixes, producer erasure fences and retention. The host supplies the catalog, dedicated database connection, migrations, source grant and maintenance schedule; installation activates none of these.
- Provide a legacy Redis collector that never asserts complete coverage, and a fake for host tests. Existing REST interfaces and defaults are unchanged.

## 0.2.0 - 2026-11-25

- `signals:api-activity` sends a coverage frame (contract 2.0.0) every run, also when the API had no calls, so Signals can learn from `api.activity` windows and tell an idle API from a dead sender. `event_time.through` stays short of every window Signals has not accepted, never moves backwards, and holds while a send fails so the source reads stale. A late part that cannot be sent, or a refused record, makes the frame partial from that window. See "Coverage frames" in the README.
- The default `grace` (`SIGNALS_API_GRACE`) is now 60 seconds instead of 120. With 120 the frame cannot pass a window's end until 120 s after it, and Signals would read the source as stale for a moment every window. Applications that set `SIGNALS_API_GRACE` above 60 should lower it.
- `RedisApiActivityLog::flush()` also returns `coverage`: the completeness of the frame Signals accepted, or null.

## 0.1.0 - 2026-11-23

First release.


- Add the `CountApiActivity` middleware: API calls are counted per actor in Redis after the response (never in the database) and sent by `signals:api-activity` as closed `api.activity` windows (contract 2.6.0) to the canonical v2 source intake, with an `ApiActorResolver` extension point, keyed pseudonyms for IP addresses and path parameters, and the `FakeApiActivity` test double.
- Add a separate typed count-interval client with exact uint64 strings, explicit coverage and privacy dependencies, immutable corrections and historical pagination. Public callers cannot grant themselves interval retention or register series.
- Verify the interval client against the actual engine HTTP handler and isolated PostgreSQL, including correction availability, scoped access, erasure and usage accounting.
- Preserve optional tenant identity in entity serialization and signal reads; legacy entity construction and wire shape remain valid.
- Synchronize the additive security/risk schemas, including the bounded risk finding response. Security risk integration remains available through the documented HTTP boundary.
- Typed events, metrics, evaluation, replay and read contracts for Cbox Signals.
- Durable outbox transport and a signed receiver inbox with duplicate, ordering, rotation and expiry handling.
- Scoped entity erasure status, configuration acknowledgement and recent normalized observations.
- Independent PHP validation of shared wire fixtures, durable receiver tests and PHP 8.4/8.5 with Laravel 12/13 CI.
