# Laravel Signals Client

Typed event, metric, login-evaluation and replay transport for Cbox Signals, plus a
transactional event outbox and a signature-verified signal inbox. PHP 8.4+ and
Laravel 12/13.

## Configure

Install the package with `composer require cboxdk/laravel-signals-client`.
Laravel discovers `SignalsServiceProvider`. Publish its configuration and migrations:

```sh
php artisan vendor:publish --tag=signals-config
php artisan vendor:publish --tag=signals-migrations
php artisan migrate
```

Set `SIGNALS_URL`, `SIGNALS_TOKEN` and `SIGNALS_TIMEOUT` according to `config/signals.php`.
A key selects one project/environment and its permitted sources. Never put keys in
client-side code. The public API uses HTTPS; isolated loopback tests may use HTTP.
The receiver checks project and environment against its own configuration.

## Events

Resolve `Cbox\Signals\Contracts\Signals` through the container:

```php
use Cbox\Signals\Contracts\Signals;
use Cbox\Signals\Data\Event;
use Cbox\Signals\Data\EventBatch;

$receipt = app(Signals::class)->events(
    new EventBatch('shop', [new Event(
        $occurrenceId,
        'shop.document_blocked',
        1,
        new DateTimeImmutable,
        ['customer' => $customerId, 'document' => $documentId],
    )]),
    $batchId,
);
```

Persist occurrence and batch identities before the first send. Retrying the same
identity must send identical bytes and timestamps; changed content produces 409.
A 202 receipt means durable acceptance, not completed detection. Read its operation
through `GET /v1/operations/{id}`. Keys need `write` and the specific source grant.
Only newly accepted occurrences consume the `signals.events` allowance.

When events accompany business writes, resolve `Contracts\EventOutbox` and call
`enqueue($batch, $batchId)` with a persisted UUID in the same database transaction. Run
`php artisan signals:dispatch-events` from the application's scheduler. The outbox keeps
an identical request across retries and marks it sent only after durable acceptance.
The outbox and the business write must use the same database connection.

## Metrics and replay

`metrics(MetricBatch $batch, string $key): Receipt` accepts typed `MetricSample`s.
Each sample has an occurrence ID, metric name, numeric value, unit, `MetricKind`,
observation time, `Entity` and named dimensions. Cumulative/delta counters also
carry interval boundaries when required by the engine. Empty dimensions serialize
as a JSON object. Preserve instance, deployment and restart dimensions so a new
process does not inherit the previous process's memory baseline.

`replay(ReplayRequest $request, string $key): Receipt` requires `manage`. Choose
explicit `from`, `to`, configuration revision, mapping version and detector ID.
The current contract permits a maximum seven-day historical window. Replay returns
an operation; inspect its result through the operation endpoint. It neither emits
live deliveries nor bills previously accepted samples again.

## Login evaluation

`evaluate(EvaluationRequest $request): Evaluation` requires `evaluate`. Persist one
`requestId` for the actual login attempt and retain it across transport retries.
Use server-observed device/session/IP context, not browser assertions. The only
current action is `auth.login`.

`evaluated`, `insufficient_data` and `degraded` are different states. A transport,
quota or malformed-response failure throws `SignalsFailure` with `errorCode` and
HTTP `status`. The caller owns its authentication policy and deadline; the SDK never
issues a session, bypasses MFA or interprets unavailable evidence as low risk.
A signal's expiry and revision are relevant to avoiding repeated challenges.

## Receive signed signals

Keep the exact raw request body. Resolve `Webhooks\VerifySignalWebhook`, pass the
`X-Cbox-Signature` header, a `Cbox\WebhookSignature\ValueObjects\SecretSet`, the
receiver's configured project/environment and the current Unix timestamp. It uses
the shared Cbox signature package, validates five-minute freshness and scope, and
returns a typed `SignalEnvelope`.

Pass that envelope to `Contracts\SignalInbox::accept()` before returning 2xx. The
inbox stores delivery identity/body hash and monotonically advances signal revision
in a transaction. An identical retry returns false; different bytes under the same
delivery identity produce 409. Older revisions never replace newer state.
`active($project, $environment)` excludes resolved/expired or time-expired signals.
Perform dependent local state changes in that same connection's transaction. Do not
interpret a duplicate result as a new action or create financial side effects.

Rotate signing keys with an overlap: configure old+new keys at the receiver, switch
the sender, verify delivery with the new key, then remove the old key. After removal,
old-key signatures fail. Never put signing secrets in logs or queue metadata.

## API activity windows

`Cbox\Signals\ApiActivity\CountApiActivity` counts an application's API calls per
actor and sends them as closed `api.activity` windows (contract 2.6.0), with a coverage
frame every minute. Add it to the API middleware group and schedule the sender every minute:

```php
// bootstrap/app.php
$middleware->appendToGroup('api', \Cbox\Signals\ApiActivity\CountApiActivity::class);
// routes/console.php
Schedule::command('signals:api-activity')->everyMinute()->withoutOverlapping();
```

Configure `signals.api_activity` (see `config/signals.php`). Counting needs
`SIGNALS_API_ACTIVITY=true`, `SIGNALS_REFERENCE_KEY` with its id, the application id and
the entity namespace; sending also needs the project, environment, source and generation
of an explicit v2 source grant for the `api.activity` family, and a `SIGNALS_TOKEN` with
write access to it. Records go to `POST /v2/sources/{source}/records:batch`.

A request never waits on Signals. `handle` only notes the start time; the call is counted
in `terminate`, after the response has been sent, by one Lua script call to Redis (or
Valkey) that adds it to that window's counters. The application database is never
touched. Counting failures, such as Redis being down, are reported at most once a minute
per process and never thrown; the calls of that time are not counted. Only requests that matched a
route are counted, by their route template (`/v1/incidents/{incident}`), never the path.

What leaves the application: the user, organization and token as entities with their
internal ids (a token's database id, never its secret); the client IP and the
path-parameter values only as 24-character HMAC-SHA256 pseudonyms under
`SIGNALS_REFERENCE_KEY`, named by `reference_key_id`. Rotate the key with a new id.

Who the actor is comes from `Contracts\ApiActorResolver`. The default takes the
authenticated user, the key of its Sanctum `currentAccessToken()` or Passport `token()`
model, and `$request->ip()`. Bind your own to add the organization, device or network
context:

```php
$this->app->bind(ApiActorResolver::class, fn () => new class implements ApiActorResolver {
    public function resolve(Request $request): ApiActor
    {
        $user = $request->user();

        return new ApiActor(
            user: $user ? (string) $user->getKey() : null,
            organization: $user?->organization_id !== null ? (string) $user->organization_id : null,
            token: $user?->currentAccessToken()?->getKey() !== null ? (string) $user->currentAccessToken()->getKey() : null,
            ip: $request->ip(),
        );
    }
});
```

### Several servers, and a server that dies

Every application server and worker counts into the same Redis (`SIGNALS_API_REDIS`
names the connection; use a dedicated one with a short `read_timeout`). The counters are
aggregates, so Redis memory follows actors, operations and references, never requests.
Each window is split into `shards` key spaces by actor, each under one hash tag, so a
Redis Cluster keeps each shard on one node.

Each shard counts into a numbered part. `signals:api-activity` seals a part `grace`
seconds (default 60) after its window ends, in one atomic script, after which calls go to
the next part; so a call lands in exactly one part. The command reads the sealed part,
builds the records, reserves their source positions once, sends them, and only then
deletes the part's keys. A retry rebuilds the same records with the same ids, positions
and `recorded_at`, so Signals counts them once. Two schedulers running at once take turns
on a lock. A call counted after its window was sealed (a slow request, a server that
comes back) becomes a later part with its own record ids; Signals adds their counts and
unites their references, so keep late records rare (each accepted record is one billed
event). A server that dies loses only the calls it had not yet counted, and never causes
a call to be counted twice. A page that Signals refuses is resent record by record; a
refused record is kept in the Redis list `signals:api:failed` (newest 1000).

### Coverage frames

Signals learns only from windows a coverage frame declares complete, and tells an idle
API from a dead sender by the frame's age. After sending records, every run of
`signals:api-activity` sends one coverage frame (contract 2.0.0) to the same endpoint,
also when there were no calls: "Signals has every record whose window ends by
`event_time.through`", with `commit_through` the highest position Signals accepted.

- A record counts in the window it closes, so `through` stays a second short of the end
  of every window Signals has not accepted: the window still counting or in its grace,
  and a sealed part waiting to be sent. Once a window's parts are accepted, or it closed
  without calls, `through` moves on to now. A window without calls is complete with
  observed zero.
- `through` never moves backwards and is stored in Redis before the frame is sent. While
  a window cannot be sent `through` holds, and after two minutes Signals reads the
  source as stale, as it would a dead sender.
- A complete frame claims from the start of the lookback (or from the first frame this
  client sent), so a window is complete again after a late part was accepted. While a
  late part cannot be sent, or when Signals refused a record, the frame is `partial`
  from that window on (`late_records_pending`, `records_refused`); a window with a
  refused record is not claimed again. After Signals refused a frame, frames claim no
  commit position (`commit_unknown`) until a record is accepted.

Signals reads a source as stale when `through` is more than 120 s old. `through` passes a
window's end only at the first run after its grace, so keep `grace` at 60 s or less and
the window a multiple of 60 s, and do not let other scheduled commands delay this one
much (`->runInBackground()` helps). The frame names the family `api.activity`, so the
source grant must have exactly that family.

### Limits

Per window: the 500 busiest actors (`max_actors`) get a window each; other actors and
calls with no user, organization, token, address or device are folded into `remainder`
windows per network context. Beyond 16 networks the busiest 15 keep one each and the
rest share one per enrichment version, which keeps only the context fields all of them
share (`network_class: unknown` and nulls across networks). The 32 busiest operations are listed and
the rest counted in `omitted_requests`. Each operation keeps the 256 smallest parameter
references (the same objects on every server), the window keeps at most 768 shared
across its operations, and every answered request whose set was not kept counts in
`overflow`. The order of calls is a prefix: 256 runs at most, ending just before the first
call to an operation that is not listed.

Redis bounds per window and shard: `tracked_actors / shards` actors (default 2000 over
8 shards) are counted one by one and later ones per network (64, then one per enrichment
version, with HyperLogLog actor counts); 32 operations per actor; the 256 smallest
references per operation and 768 per actor; the first 1024 calls per actor for the order.

`tests/ApiActivityRedisTest.php` proves exactly-once counting with concurrent writers and
flushers, the retry, and the memory bound under 20000 addresses; set
`SIGNALS_TEST_REDIS=redis://host:6379/15` to run it (CI does; the database is flushed).

### Testing

```php
$fake = FakeApiActivity::swap($this->app);
$this->getJson('/v1/incidents/42');
$fake->assertSent(fn (array $window) => $window['operations'][0]['route'] === '/v1/incidents/{incident}');
$fake->records(); // the complete 2.6.0 records Signals would receive
```

## Contracts and verification

Canonical wire schemas and OpenAPI live in `cboxdk/signalsd/contracts`.
`tests/Contracts` carries exact schema/fixture copies for independent PHP validation
using Opis JSON Schema 2.6.0; Go validates the same fixtures with its independent
Draft 2020-12 implementation. Synchronize both copies when changing a contract.

```sh
composer qa
composer sbom
```

The app's `tests/Integration/sdk-metrics-roundtrip.py` runs this actual SDK against
isolated PostgreSQL/NATS/ClickHouse-backed engine services. The engine's
`TestActualPHPReceiverRetryRotationAndExpiry` starts the actual PHP inbox service,
verifies TLS, commit-before-failed-ack retry, key overlap/removal, duplicate and
out-of-order delivery, expiry and foreign-scope rejection. HTTP fakes in unit tests
are reserved for deterministic failure cases; they are not integration evidence.

Do not enable live authentication enforcement or production payments from these
local proofs. See the application's `BUILD-STATUS.md` for remaining pilot gates.

## Test doubles

`Testing\FakeSignals` implements the same `Contracts\Signals` interface. Bind it
explicitly in a test and queue each expected result with `queueReceipt('events',
$receipt)`, `queueReceipt('metrics', $receipt)`, `queueReceipt('replay', $receipt)` or
`queueEvaluation($evaluation)`. Each queue also accepts an exception to simulate
failure. Missing responses throw `LogicException`; the fake never invents success.
Inspect `eventCalls`, `metricCalls`, `replayCalls` and `evaluationCalls` to verify
payloads and stable retry keys. Production service providers never select this fake.

### Follow accepted work and erase an entity

`Signals::operation($receipt->operationId)` returns an `Operation` with an
`OperationState` (`Accepted`, `Completed`, `Failed`), accepted count, nullable error
code and typed isolated `ReplayResult`. Acceptance is not completion. Poll with a
bounded deadline/backoff appropriate to the caller; a failed operation is not a
successful zero-result replay. Replay responses declaring live deliveries or
billable events are rejected as malformed.

`Signals::erase(new Entity(...))` requires a `manage` key. Retry the same entity to
obtain the same request ID; use `Signals::erasure($id)` to follow `Pending`,
`PendingHistory`, and (when configuration acknowledgement is required)
`PendingConfiguration`. Only `Completed` confirms that operational and analytical
cleanup and all required configuration acknowledgements have finished. Usage/deduplication records survive, and the
entity tombstone refuses new ingestion. This API does not delete a whole project
or retroactively remove encrypted backup copies; follow the service's recovery
and retention procedure for those.

All read/status responses use the same bounded authenticated transport and refuse
redirects. Malformed states and invalid RFC3339 timestamps raise `SignalsFailure`
instead of returning invented status or leaking a date-parser exception. Test
fakes require explicit `queueOperation()` and `queueErasure('erase'|'erasure', …)`
responses; exhausted queues throw.

### Read signals, evidence and source health

Use a key with `read` scope. All results remain confined to that key's project and
environment; these methods cannot select another tenant.

```php
use Cbox\Signals\Data\SignalFilter;

$page = $signals->signals(new SignalFilter(status: 'active', severity: 'high'));
foreach ($page->data as $finding) {
    $detail = $signals->signal($finding->id);
    // $detail->signal->evidence contains typed measures and occurrence references.
    // $detail->revisions and ->deliveries contain bounded history, never secrets.
}
if ($page->nextCursor !== null) {
    $next = $signals->signals(new SignalFilter(after: $page->nextCursor, status: 'active', severity: 'high'));
}
$detectors = $signals->detectors();
$connections = $signals->connections();
```

`SignalPage` contains at most 50 `SignalRecord` values and an opaque nullable
cursor. Preserve the same filters when following it. Pagination is explicit, so a
caller controls its own work/deadline. `SignalDetail` reports `historyLimit` (100),
revision history and delivery metadata. It is not an unbounded audit export.
Metric findings have `mappingVersion=0` because no event mapping was applied.

`DetectorState` retains per-quality counts, entity count and nullable data time.
`ConnectionState` reports actual imports, budget, last attempt/success/event times,
nullable error and pending operation. No checkpoint rows means no imported source
history; an empty list does not imply that configured sources are healthy. Missing
samples, degraded quality and expired signals must remain visible to consumers.
Read methods never change payments, subscriptions, sessions or detector state.

Test fakes expose `queueSignals`, `queueSignal`, `queueDetectors` and
`queueConnections`, corresponding call logs, exception queues and exhaustion
errors. `tests/ReadTest.php` verifies pagination, historical evidence, incomplete
states, malformed responses and access refusal. The real SDK acceptance additionally
reads persisted metric findings/details/status before replay and entity erasure.

`$signals->retention()` reads the credential-scoped policy and operational/history
cleanup watermarks. `pending_history` means ClickHouse cleanup is not yet confirmed;
a configured number of retention days alone is not evidence of completed cleanup.
Read access is required. The test fake requires `queueRetention($state)` or an
explicit exception; it never supplies a fabricated ready state.

Signal detail exposes the latest retained normalized activity for its subject as
`$detail->recentObservations`. Each entry carries the source, event ID, type and
event time. Use the signal's own evidence for its historical decision.

### Closed count intervals and historical revisions

`Cbox\Signals\Contracts\CountIntervals` sends and reads registered count series
through the public API. It uses the existing `signals.url`, `signals.token` and
timeout configuration. It is separate from `Signals`; existing consumers and
test fakes keep their existing contract. The service control plane must first
authorize the source, interval retention and immutable series definition. A
public client cannot create that authorization.

An interval contains a closed source observation, not a running total or a
forecast. Persist the exact `CountInterval::toArray()` representation before the
first send; retry that same revision after an uncertain response. All timestamps
are explicit UTC strings, preserving source nanoseconds. Counts are decimal
strings through `18446744073709551615`, never PHP floats or signed integers.
Coverage is required: observed zero is `'0'`; unknown is `null` with an `unknown`
coverage reason. A partial count retains `partial` coverage.

```php
use Cbox\Signals\Contracts\CountIntervals;
use Cbox\Signals\Data\CountInterval;
use Cbox\Signals\Data\IntervalHistoryQuery;
use Cbox\Signals\Data\IntervalSeries;

$intervals = app(CountIntervals::class);
$series = new IntervalSeries('api', 'requests', 1);
$record = CountInterval::fromArray($persistedSourceInterval);
$receipt = $intervals->ingest($series, [$record]);

$page = $intervals->history($series, new IntervalHistoryQuery($from, $through));
if ($page->nextCursor !== null) {
    $next = $intervals->history($series, new IntervalHistoryQuery(
        $from, $through, $page->asOf, $page->nextCursor,
    ));
}
```

Each record carries explicit project/environment/tenant scope, an opaque entity,
registered dimensions and privacy subjects. Include the owners whose erasure
must remove an aggregate; dropping a former owner from a correction cannot
revive erased history. IP, session and device identities are not interval keys.
Use internal key identifiers, never bearer tokens or payment credentials.

A correction uses the next revision and the prior acknowledgement's `hash` as
`previous_hash`; it cannot overwrite its predecessor. `knownAt` is the server's
actual observation time. Preserve a page's `asOf` while following its cursor or
repeating a historical query, so later imports/corrections cannot leak into an
earlier backtest. Current retention and privacy rules still apply to that query.
Missing buckets remain absent; this client never fills them with zero.

Ingest allows 1–1,000 records, 16 KiB each and one MiB per request. History pages
contain at most 1,000 records and have a bounded streamed response. Pagination and
retry are explicit. `SignalsFailure` preserves recognized error codes and HTTP
status without leaking response details. In particular, HTTP 410 means retired
history; it is not a successful duplicate and must not be retried as a new event.
The client provides transport and typed history; it does not train a model,
certify source coverage or perform a forecast.

The real acceptance test runs this PHP client against the selected engine's
actual HTTP handler and PostgreSQL store. Set `SIGNALS_TEST_DATABASE_URL` privately
to a disposable loopback PostgreSQL instance, then run:

```sh
python3 bin/test-count-intervals.py \
  --engine /absolute/path/to/signalsd \
  --output /private/interval-acceptance.json
```

The runner creates and removes its own schema. It checks exact counts and source
timestamps, duplicates, corrections, historical pagination, foreign credentials,
unknown/partial coverage, entity erasure and preserved project usage totals.
It uses synthetic data and isolated allowances; it is not production data or
model-quality evidence. Existing receipts are never overwritten.

## Bundled managed integration (first-party source apps)

Cbox ID, Cbox Billing and Stache do not configure Signals by hand. Each
application enrols its own environments in a *managed* integration: Signals
finds or creates the organization's project, stores the read secret encrypted
and registers the catalog's default shadow pipelines. Enrolment is idempotent,
a new secret is a rotation, and revoking stops pulling within one worker
interval. Customers upgrade to custom pipelines in Signals on top of the same
sources; the bundled level is never enforcement.

The application contributes one class and a credential:

```php
final class IdManagedSources implements \Cbox\Signals\Contracts\ManagedSources
{
    public function provider(): string { return 'cbox_id'; }
    public function environments(): iterable { /* live environments → ManagedEnvironment */ }
    public function retired(): iterable { /* owner ids of deleted environments */ }
    public function issueSecret(ManagedEnvironment $environment): IssuedSecret { /* export-scoped read key */ }
    public function retireSecrets(ManagedEnvironment $environment, string $keepReference): void { /* revoke the rest */ }
    public function discardSecret(ManagedEnvironment $environment, string $reference): void { /* revoke one Signals never accepted */ }
}

$this->app->bind(ManagedSources::class, IdManagedSources::class);
$this->app->bind(ManagedCredential::class, fn () => new MachineTokenCredential(...)); // Cbox ID client-credentials token
```

Then schedule `signals:managed-reconcile` (hourly is plenty) and call
`ManagedEnroller::enroll()` / `revoke()` from environment lifecycle hooks when
you want Signals to follow within seconds. The reconcile leaves matching
enrolments untouched, so a run without changes makes no writes; pass
`--rotate=<owner>` to replace a secret. Secrets are issued only when Signals is
about to receive them, the previous ones are retired only after Signals
accepted the replacement, and a secret Signals refused is discarded at once. `FakeManagedIntegration` carries the same rules for
application tests.

`SIGNALS_MANAGED_TOKEN` (falling back to `SIGNALS_TOKEN`) is only the default
credential; bind your own `ManagedCredential` to use a Cbox ID machine token.

## MCP activity (Signals 2.8.0)

### Durable admission and complete coverage

`McpActivity\DatabaseMcpActivityLog` implements `McpActivityLog` using PostgreSQL
and the bundled MCP migration. Supply a **dedicated connection to the same
database as the privacy registry**, outside application transactions. SQLite is
supported for local tests only. The host must not execute a tool if `start()`
throws or returns null. Database admission time, before tool lookup/validation,
is the start time; a caller's timestamp cannot insert into an already closed
interval. A registration proves an admitted attempt, not execution of a mutation.

`finish()` commits one terminal outcome; a deadline without a durable result
becomes `unknown`. `flush()` seals closed windows only after their attempts have
finalized, atomically assigns them to immutable parts, and sends up to 100 parts
plus one finite coverage interval. The SQL outbox survives Redis loss and retries
identical identities/payloads. A source-scoped PostgreSQL session lock serializes
senders, while admission's short transactions do not wait for HTTP delivery.

Set `Settings::dispatchProofConfirmed` only after the host verifies **all serving
dispatchers** use this admission boundary. It defaults to false; unconfirmed
inventory reports `dispatch_inventory_unconfirmed`, never complete. A changed
assertion requires a fresh source generation, as do unobserved deployments,
collector-disable periods and SQL restore/failover that may have lost committed
writes. The library cannot detect an uninstrumented process or prove that a
restored database contains every historical commit.

Complete coverage requires a closed interval whose admitted count, sealed count
and part counts reconcile, every part acknowledged, and no pending attempts,
unknown outcomes, omitted attempts or invalidated evidence. A proved empty
interval uses source position zero without creating an event. The initial
partially observed window remains partial. This measures **admitted attempts**,
not all offered HTTP/MCP traffic or service availability. Reference/sequence caps
retain their separate quality limits even when delivery is complete.

`status()` reports backlog and the last acknowledged finite coverage interval.
Heartbeat retries preserve their bytes; fresh replacement frames use actual send
time. Lost parts, privacy erasure and retention invalidate their intervals.
Coverage can recover for later good intervals without repairing earlier gaps.
Publish the `signals-migrations` tag and run its MCP migration before enabling admission.
It creates new tables; it does not upgrade tables from unpublished development drafts.

Capacity defaults to 10,000 pending attempts **plus retained parts**, including
acknowledged parts kept for reconciliation and erasure. Full capacity refuses
new admission. Logical retention defaults to seven days (two hours to seven
days configurable); SQL has no automatic TTL. The host must run flush/maintenance
regularly for physical pruning, including draining old enabled generations before
retirement. Up to 100 zero-window manifests are discovered per flush, with at most
20,000 retained manifests and 128 registered SQL sources. No scheduler is installed.

`McpActivityBuffers::erase()` removes SQL payloads and queues proof withdrawal in
the same transaction as its tombstone, before legacy Redis cleanup. Ownership
fingerprints are indexed alongside payloads and restored data is checked again.
Conservatively the entire affected producer window is invalidated, which can
also remove other actors in that window. The host must still call the engine's
erasure API for accepted data and requests already in flight. Never restore an
old source registry/tombstone snapshot and resume its complete claims.

### Legacy fail-open journal

`McpActivity\RedisMcpActivityLog` implements `Contracts\McpActivityLog` for
Signals 2.8.0. A host supplies a server-owned `Catalog` of `Operation`
values and a **separate** `McpActivity\Settings`/`ApiActivitySettings` source;
it must not replace the REST collector's container binding. Register the host's
dispatcher observer before tool lookup. `Attempt::capture()` retains only
catalogued names, actor identity and keyed pseudonyms of complete allowlisted
string object-id sets; raw arguments, results and IP addresses are not stored.

Call `start($attempt)` once per dispatched tool attempt, then `finish($id,
$outcome, $replayed)` on the observed terminal result. Null registration means
the bounded journal refused the attempt. Progress is not a terminal result;
consume a stream before declaring success. `FakeMcpActivityLog` is the host-test
double, not a simulation of Redis deadlines. It is not installed automatically.

An explicitly invoked `flush(SourceRecords $sender)` expires unresolved attempts
to `unknown`, seals closed start-time windows and sends at most 100 records.
Redis atomically chooses one finalization. Delivery retries retain the exact
record identity and source position; a sender restart can find pending records
older than one hour. A changed catalog policy, key (even reused key id), scope,
namespace, deadline, window or retention cannot adopt an existing generation.
Drain with the original configuration, then use a new generation.

Defaults: a 300-second outcome deadline, at most 10,000 pending attempts plus
sealed records, and seven-day logical buffer retention (configurable from two
hours to seven days). Registration prunes expired pending entries; flushing also
prunes expired sealed records before sending. Shared payload keys have Redis TTL
so an abandoned collector cannot retain them forever; during inactivity an old
entry can physically remain until the latest entry in that shared key expires.
Run the explicit flush regularly for timely physical pruning. Configuration
fingerprints and sequence counters remain without actor payload, preventing
source-position reuse after expiry. Expiry counters are **lower bounds** because
Redis TTL may remove idle data before the collector counts it.

The journal capacity bounds distinct actors too; it does not reuse REST's
`maxActors` setting or fold excess named actors. Excess registrations increment
`refused`. Unidentified actors form remainder windows. Within each record there
are at most 32 operations, 256 references per operation/768 total, 256 sequence
runs and 1024 represented attempts. Byte-limit reduction preserves omission
and overflow counts. One accepted window part remains one billable event.

Each successful flush also sends a contract 2.0.0 family-coverage heartbeat.
It reports the acknowledged source position and `unknown` coverage with
`collector_fail_open`, or `partial` when refusals, expiry, erasure, unknown
outcomes or omitted attempts are known. Pending records are explicit. Counters
are cumulative per generation; they do not claim that every loss occurred in
the heartbeat's lookback. `omitted_records` counts only discarded sealed windows,
never attempts. Uncertain heartbeat delivery retries the same frame; stale frames
are replaced. A consumer-rejected commit is retracted to zero, with
`commit_unknown`, until a later record is acknowledged.

**No complete training coverage is asserted.** A successful flush, an empty queue
or zero traffic cannot prove that fail-open dispatch registration did not lose
calls. Complete coverage requires independently durable dispatch evidence; do
not switch the heartbeat to `complete` merely because Redis has recovered.

Publish the new `signals-migrations` migration and supply
`Contracts\McpActivityBuffers` as the journal's third constructor argument.
The default `DatabaseMcpActivityBuffers` binding uses the host database and Redis.
It durably registers each buffer before writing payloads and uses a database row
lock across registration, finalization, delivery and erasure. Call it outside an
outer application transaction so registration and erasure fences commit before
Redis work. PostgreSQL concurrency is verified; in-memory SQLite tests alone are
not concurrency evidence. All workers must use the same database/registry and
stable Redis connection names. Do not change a registered connection's target;
give a moved Redis database a new connection name and keep the old one reachable
until its buffers have been erased or drained. The registry admits at most 128
buffer identities; drain and explicitly retire obsolete entries before adding more.

On verified privacy erasure, call `McpActivityBuffers::erase(Entity)` even when
collection is disabled. It persists a one-way identity fingerprint, removes
matching pending and sealed data across registered sources/generations, and
refuses later registration. Cleanup failures propagate for retry; the fence
survives Redis failure and is checked again before accessing restored buffers.
Without named entity provenance, remainder/IP/device-only windows starting at or
before an erasure are conservatively removed too, including unrelated anonymous
history. Token revocation and organization archival are **not** privacy erasure.

This handles producer buffers only. The host must also use the existing Signals
erasure API for accepted data and derived models; its fence is required for an
HTTP request already in flight when a sender loses its database connection.
The serialized flush can delay registration, so production throughput and outage
qualification remain required. No old REST buffers are recovered or migrated.

Verify using a disposable Redis database only:
`SIGNALS_TEST_REDIS=redis://127.0.0.1:6379/14 vendor/bin/pest` (the tests clear it).
